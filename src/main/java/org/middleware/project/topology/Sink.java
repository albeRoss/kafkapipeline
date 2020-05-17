package org.middleware.project.topology;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.middleware.project.utils.ConsoleColors;

public class Sink implements Runnable {

    private  boolean firstWrite = true;
    private static final int waitBetweenMsgs = 500;
    private final String  inTopic;
    private String boostrapServers;
    private volatile boolean running;
    private KafkaConsumer<String, String> consumer;
    private String console = ConsoleColors.WHITE_UNDERLINED + "[SINK] \t";

    public Sink(Properties properties) {

        this.inTopic = properties.getProperty("inTopic");
        this.boostrapServers = properties.getProperty("bootstrap.servers");
        this.running = true;

        init();

    }

    private void init() {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", boostrapServers);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("isolation.level", "read_committed");
        consumerProps.put("group.id", "sinkGroup");
        consumerProps.put("enable.auto.commit", "false");

        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singleton(inTopic));

        System.out.println(console+"Sink initialized");
    }


    private final void writeMessage(String record) {
        try (FileWriter writer = new FileWriter(new File("sink.txt"), true)) {

            StringBuilder sb = new StringBuilder();

            // If is first write, write header first
            if(firstWrite) {
                sb.append("SINK RECORDS");
                sb.append('\n');
                firstWrite = false;
            }

            // Write message on txt
            sb.append(record);
            sb.append('\n');

            writer.write(sb.toString());

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
            try {
                while (running) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.of(30, ChronoUnit.SECONDS));
                    for (final ConsumerRecord<String, String> record : records) {
                        String sinkRecord = "Partition: " + record.partition() + ".\t" + //
                                "Offset: " + record.offset() + ".\t" + //
                                "Key: " + record.key() + ".\t" + //
                                "Value: " + record.value();

                        System.out.println(console+"SINK: " + ".\t" + //
                                sinkRecord);

                        // There is also an asynchronous version that invokes a callback
                        consumer.commitSync();
                        writeMessage(sinkRecord);
                    }
                }
                try {

                    Thread.sleep(waitBetweenMsgs);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

        } catch (KafkaException e) {
            System.out.println(console+"Sink failed. Try again.");
        }
        System.out.println(console+"sink closing");
        consumer.close();


    }
}

