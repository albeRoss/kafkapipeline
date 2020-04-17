package org.middleware.project.topology;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.middleware.project.processors.Processor;

public class Source implements Processor {

    private static final boolean print = true;
    private static final int waitBetweenMsgs = 500;
    private final String outTopic;
    private String boostrapServers;
    private final String transactionId;
    private volatile boolean running;
    private KafkaProducer<String, String> producer;

    public Source(Properties properties) {

        this.outTopic = properties.getProperty("outTopic");
        this.boostrapServers = properties.getProperty("bootstrap.servers");
        this.transactionId = properties.getProperty("transactionId");
        init();

    }

   public void init(){


       final Properties props = new Properties();
       props.put("bootstrap.servers", boostrapServers);
       props.put("key.serializer", StringSerializer.class.getName());
       props.put("value.serializer", StringSerializer.class.getName());
       // Idempotence = exactly once semantics between producer and partition
       props.put("enable.idempotence", true);

       this.producer = new KafkaProducer<>(props);

       }

    @Override
    public void process() {

    }

    @Override
    public void punctuate() {

    }

    @Override
    public void run() {
        final List<String> topics = Collections.singletonList("source");
        final int numMessages = 20;
        final Random r = new Random();

        // This must be called before any method that involves transactions
        producer.initTransactions();

        try{
            producer.beginTransaction();
            for (int i = 0; i < numMessages; i++) {
                final String topic = topics.get(r.nextInt(topics.size()));
                final String key = "Key" + r.nextInt(1000);
                final String value = String.valueOf(i);
                if (print) {
                    System.out.println("Topic: " + topic + "\t" + //
                            "Key: " + key + "\t" + //
                            "Value: " + value);
                }

                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record);
            }

            producer.commitTransaction();
        }catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();

        try {
            Thread.sleep(waitBetweenMsgs);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

    }
}

