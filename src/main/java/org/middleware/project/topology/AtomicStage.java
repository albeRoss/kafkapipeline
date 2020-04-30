package org.middleware.project.topology;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.middleware.project.Processors.*;
import org.middleware.project.functions.FlatMap;
import org.middleware.project.functions.WindowedAggregate;

public class AtomicStage implements Processor {

    //private static final int numRepetitions = 200;

    protected final String group;
    protected final String inTopic;
    protected final String outTopic;
    protected String boostrapServers;
    protected final String transactionId;
    protected final String stage_function;
    protected final StageProcessor stageProcessor;
    protected volatile boolean running;

    protected KafkaProducer<String, String> producer;
    protected KafkaConsumer<String, String> consumer;
    protected final int id;

    public AtomicStage(Properties properties, int id, StageProcessor stageProcessor) {
        this.group = properties.getProperty("group.id");
        this.inTopic = properties.getProperty("inTopic");
        this.outTopic = properties.getProperty("outTopic");
        this.boostrapServers = properties.getProperty("bootstrap.servers");
        this.stageProcessor = stageProcessor;
        this.transactionId = "atomic_forwarder_" + properties.getProperty("group.id") + "_transactional_id_" + id;
        this.stage_function = stageProcessor.getClass().getSimpleName();
        this.id = id;
        running = true;
        System.out.println("[ ATOMICSTAGE : "+this.stage_function+" ]" +"\t group = " + group +"\t inTopic = " + inTopic
                +"\t outTopic = " + outTopic+"\t boostrapServers = " + boostrapServers);
        //System.out.println("\t transactionId = " + transactionId);

        init();
    }

    @Override
    public void init() {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", boostrapServers);
        consumerProps.put("group.id", group);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("isolation.level", "read_committed");
        consumerProps.put("enable.auto.commit", "false");


        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singleton(inTopic));
        //System.out.println("subscribed to : " + inTopic);

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", boostrapServers);
        producerProps.put("group.id", group);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("transactional.id", transactionId);
        producerProps.put("enable.idempotence", true);

        this.producer = new KafkaProducer<>(producerProps);
    }

    @Override
    public void process(final ConsumerRecord<String, String> record) {

        if (stageProcessor instanceof FlatMapProcessor){
            //System.out.println("I'm a flatmap");
            HashMap<String,List<String>> processed = stageProcessor.process(record);

            processed.forEach((key,values) -> {

                for (Object item: values) {
                    String value = ((String) item);
                    this.producer.send(new ProducerRecord<>(outTopic, key, value));
                }

            });

        }else{
            HashMap<String, String> processed = stageProcessor.process(record);

            if (!processed.isEmpty()) {
                for (String key: processed.keySet()){
                    String value = processed.get(key);
                    this.producer.send(new ProducerRecord<>(outTopic,key,value));
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            this.producer.initTransactions();


            while(running) {
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                this.producer.beginTransaction();

                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("[GROUP : "+group+" ] " + "["+inTopic+"] " +
                            "[FORWARDER : "+id+" ] : "+
                            "Partition: " + record.partition() + "\t" + //
                            "Offset: " + record.offset() + "\t" + //
                            "Key: " + record.key() + "\t" + //
                            "Value: " + record.value());

                    process(record);


                    // this.producer.send(new ProducerRecord<>(outTopic, record.key(), record.value())); //replaced

                }
                // The producer manually commits the outputs for the consumer within the
                // transaction
                final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                for (final TopicPartition partition : records.partitions()) {
                    final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }

                this.producer.sendOffsetsToTransaction(map, group);
                this.producer.commitTransaction();


            }
            this.consumer.close();
            this.producer.close();


        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            System.out.println("    We can't recover from these exceptions, so our only option is to close the producer and exit.");
            producer.close();
            consumer.close();
            running = false;
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println("     aborts the transaction. Try again.");
            producer.abortTransaction();
            // retry comes for free
        } finally {
            System.out.println("    finally closing atomic forwarder at group: " + group);
            consumer.close();
            producer.close();
            running = false;

        }


    }

    public void shutdown() {
        running = false;
    }


}

