package org.middleware.project.topology;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Source {
    private static final boolean print = true;
    private static final int waitBetweenMsgs = 500;

    void Source(){

    }

   /* public static void main(String[] args) {
        final List<String> topics = Collections.singletonList("topic01");
        final int numMessages = 20;

        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // Idempotence = exactly once semantics between producer and partition
        props.put("enable.idempotence", true);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final Random r = new Random();

        for (int i = 0; i < numMessages; i++) {
            final String topic = topics.get(r.nextInt(topics.size()));
            final String key = "Key" + r.nextInt(1000);
            final String value = String.valueOf(i);
            if (print) {
                System.out.println("Topic: " + topic + "\t" + //
                        "Key: " + key + "\t" + //
                        "Value: " + value);
            }
            producer.send(new ProducerRecord<>(topic, key, value));

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();

    }*/
   void init(){
       final List<String> topics = Collections.singletonList("topic01");
       final int numMessages = 20;

       final Properties props = new Properties();
       props.put("bootstrap.servers", "localhost:9092");
       props.put("key.serializer", StringSerializer.class.getName());
       props.put("value.serializer", StringSerializer.class.getName());
       // Idempotence = exactly once semantics between producer and partition
       props.put("enable.idempotence", true);

       final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
       final Random r = new Random();

       for (int i = 0; i < numMessages; i++) {
           final String topic = topics.get(r.nextInt(topics.size()));
           final String key = "Key" + r.nextInt(1000);
           final String value = String.valueOf(i);
           if (print) {
               System.out.println("Topic: " + topic + "\t" + //
                       "Key: " + key + "\t" + //
                       "Value: " + value);
           }
           producer.send(new ProducerRecord<>(topic, key, value));

           try {
               Thread.sleep(waitBetweenMsgs);
           } catch (final InterruptedException e) {
               e.printStackTrace();
           }
       }

       producer.close();
   }

}

