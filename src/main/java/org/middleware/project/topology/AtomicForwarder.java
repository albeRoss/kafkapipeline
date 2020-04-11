package org.middleware.project.topology;

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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.middleware.project.processors.Processor;

public class AtomicForwarder implements Processor {

    private static final int numRepetitions = 20;

    private final String group;
    private final String inTopic;
    private final String outTopic;
    private String boostrapServers;
    private final String transactionId;

    private  KafkaProducer<String, String> producer;
    private  KafkaConsumer<String, String> consumer;

    public AtomicForwarder(String group, String inTopic, String outTopic, String bootstrapServers, String transactionId) {
        this.group = group;
        this.inTopic = inTopic;
        this.outTopic = outTopic;
        this.boostrapServers = bootstrapServers;
        this.transactionId = transactionId;
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
        consumer.subscribe(Collections.singleton(inTopic));

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

        consumer.close();
        producer.close();

    }

    @Override
    public void punctuate() {

    }

    public static void main(String[] args) {

        try {
            Process process = Runtime.getRuntime().exec(new String[]{"cd " + System.getProperty("user.home"),
            System.out.println(Runtime.getRuntime().exec("cd " + System.getProperty("user.home")));
            //Runtime.getRuntime().exec("./bin/zookeeper-server-start.sh config/zookeeper.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*for (int i = 0; i < 3; i++) {
            AtomicForwarder af1 = new AtomicForwarder("group1", "topic01", "topicOut",
                    "localhost:9092","transaction".concat(String.valueOf(i)));
        }*/



    }
}

