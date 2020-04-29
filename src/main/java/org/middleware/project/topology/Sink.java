package org.middleware.project.topology;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

public class Sink implements Runnable {

    private static final boolean print = true;
    private static final int waitBetweenMsgs = 500;
    private final String  inTopic;
    private String boostrapServers;
    private volatile boolean running;
    private KafkaConsumer<String, String> consumer;

    public Sink(Properties properties) {

        this.inTopic = properties.getProperty("inTopic");
        this.boostrapServers = properties.getProperty("bootstrap.servers");

        running = true;
        System.out.println("[SINK]" + "\t" + "boostrapServers = " + boostrapServers);
        init();

    }

    private void init() {

        final Properties props = new Properties();

        props.put("bootstrap.servers", boostrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        this.consumer = new KafkaConsumer<String, String>(props);

        System.out.println("Sink initialized");
    }



    @Override
    public void run() {
            try {
                System.out.println("Topics: " + inTopic);
                consumer.subscribe(Collections.singleton(inTopic));
                while (running) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                    for (final ConsumerRecord<String, String> record : records) {
                        System.out.println("SINK: " + ".\t" + //
                                "Partition: " + record.partition() + ".\t" + //
                                "Offset: " + record.offset() + ".\t" + //
                                "Key: " + record.key() + ".\t" + //
                                "Value: " + record.value());

                        // There is also an asynchronous version that invokes a callback
                        consumer.commitSync();
                    }
                }
                try {

                    Thread.sleep(waitBetweenMsgs);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

        } catch (KafkaException e) {
            System.out.println("Sink failed. Try again.");
        }
        System.out.println("source closing");
        consumer.close();


    }
}

