package org.middleware.project.topology;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.middleware.project.processors.Processor;

public class AtomicForwarder implements Processor {

    private static final int numRepetitions = 20;

    private final String group;
    private final String inTopic;
    private final String outTopic;
    private String boostrapServers;
    private final String transactionId;
    private final String stage_function;
    private volatile boolean running;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    public AtomicForwarder(Properties properties) {
        this.group = properties.getProperty("group");
        this.inTopic = properties.getProperty("inTopic");
        this.outTopic = properties.getProperty("outTopic");
        this.boostrapServers = properties.getProperty("bootstrapServers");
        this.transactionId = properties.getProperty("transactionId");
        this.stage_function = properties.getProperty("stage.function");
        running = true;

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
    public void process() {

    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton(inTopic));
            while (running) {
                producer.initTransactions();

                for (int i = 0; i < numRepetitions; i++) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

                    producer.beginTransaction();

                    for (final ConsumerRecord<String, String> record : records) {
                        System.out.println(record.key() + " : " + record.value());
                        producer.send(new ProducerRecord<>(outTopic, record.key(), record.value()));
                    }

                    // The producer manually commits the outputs for the consumer within the
                    // transaction
                    final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(map, group);
                    producer.commitTransaction();


                }

            }
        }catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
            consumer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        finally {
            consumer.close();
            producer.close();

        }


    }

    @Override
    public void punctuate() {

    }

    public void shutdown() {
        running = false;
    }

    public static void main(String[] args){
        /*ProcessBuilder pb_z =
                new ProcessBuilder("./bin/zookeeper-server-start.sh", "config/zookeeper.properties");
        ProcessBuilder pb_k =
                new ProcessBuilder("./bin/kafka-server-start.sh", "config/server.properties");
        //Map<String, String> env = pb_z.environment();
        //Map<String, String> env = pb_z.environment();
        //env.put("VAR1", "myValue");
        //env.remove("OTHERVAR");
        //env.put("VAR2", env.get("VAR1") + "suffix");
        pb_z.directory(new File(System.getProperty("user.home") + "/Desktop/middleware/kafka_2.12-2.3.1"));
        pb_k.directory(new File(System.getProperty("user.home") + "/Desktop/middleware/kafka_2.12-2.3.1"));
        File log_z = new File("log_zookeper");
        File log_k = new File("log_kafka");
        pb_z.redirectErrorStream(true);
        pb_k.redirectErrorStream(true);
        pb_z.redirectOutput(ProcessBuilder.Redirect.appendTo(log_z));
        pb_k.redirectOutput(ProcessBuilder.Redirect.appendTo(log_k));
        Process p_z = pb_z.start();
        //Process p_k = pb_z.start();*/
    }
}

