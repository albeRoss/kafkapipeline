package org.middleware.project.topology;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import javafx.util.Pair;
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

    protected  String group;
    protected  String inTopic;
    protected  String outTopic;
    protected String boostrapServers;
    protected  String transactionId;
    protected  String stage_function;
    protected  StageProcessor stageProcessor;
    protected volatile boolean running;
    protected volatile boolean prerestart;

    protected KafkaProducer<String, String> producer;
    protected KafkaConsumer<String, String> consumer;
    protected  int id;
    protected int pos;
    private int simulateCrash;

    public AtomicStage(Properties properties, int id, StageProcessor stageProcessor, int simulateCrash) {
        this.group = properties.getProperty("group.id");
        this.inTopic = properties.getProperty("inTopic");
        this.outTopic = properties.getProperty("outTopic");
        this.boostrapServers = properties.getProperty("bootstrap.servers");
        this.stageProcessor = stageProcessor;
        this.transactionId = "atomic_forwarder_" + properties.getProperty("group.id") + "_transactional_id_" + id;
        this.stage_function = stageProcessor.getClass().getSimpleName();
        this.id = id;
        if(simulateCrash == 0) this.simulateCrash = Integer.MAX_VALUE;
        else {
            this.simulateCrash = simulateCrash;
            System.out.println("this processor will crash: \t"+ this.id +"\t"+group);
        }

        //infer position from groupId
        this.pos = Integer.parseInt(group.substring(6));

        running = true;
        System.out.println("[ ATOMICSTAGE : "+this.stage_function+" ]" +"\t group = " + group +"\t inTopic = " + inTopic
                +"\t outTopic = " + outTopic+"\t boostrapServers = " + boostrapServers);
        //System.out.println("\t transactionId = " + transactionId);

        init();
    }

    public AtomicStage() {
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
        consumerProps.put("max.poll.records","1");


        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singleton(inTopic));

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


            while (running) {
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.of(1, ChronoUnit.MINUTES));
                this.producer.beginTransaction();

                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("[GROUP : " + group + " ] " + "[" + inTopic + "] " +
                            "[FORWARDER : " + id + " ] : " +
                            "Partition: " + record.partition() + "\t" + //
                            "Offset: " + record.offset() + "\t" + //
                            "Key: " + record.key() + "\t" + //
                            "Value: " + record.value());

                    process(record);


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


                if (simulateCrash > 0) {
                    simulateCrash--;
                } else {
                    crash();
                }
                Thread.sleep(100);


            }
            this.consumer.close();
            this.producer.close();

        }catch (InterruptedException e) {
                System.out.println("AtomicStatelessStageinterrupted");

                e.printStackTrace();
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
        }


    }

    private void crash(){

        DB dbc = DBMaker.fileDB("crashedThreads.db").fileMmapEnableIfSupported().make();
        ConcurrentMap mapc = dbc.hashMap("crashedThreads").createOrOpen();

        //we need the id of the processor and the stage position

        mapc.put(id, new Pair<>(pos,"stateless"));
        dbc.close();
        producer.abortTransaction();
        consumer.close();
        producer.close();
        running = false;
        System.out.println("failure!");
        throw new RuntimeException("[failure] : "+ this.stageProcessor.getClass().getSimpleName());

    }

    public void shutdown() {
        running = false;
    }


}

