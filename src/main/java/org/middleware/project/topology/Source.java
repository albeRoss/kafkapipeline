package org.middleware.project.topology;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.middleware.project.utils.ConsoleColors;

public class Source implements Runnable {

    private static final boolean print = true;
    private static final int waitBetweenMsgs = 2000;
    private final String outTopic;
    private String boostrapServers;
    private final String transactionId;
    private volatile boolean running;
    private KafkaProducer<String, String> producer;
    private String console = ConsoleColors.WHITE_UNDERLINED + "[SOURCE] \t";

    public Source(Properties properties) {

        this.outTopic = properties.getProperty("outTopic");
        this.boostrapServers = properties.getProperty("bootstrap.servers");
        this.transactionId = properties.getProperty("transactionId");

        running = true;
        System.out.println(console + "outTopic = " + outTopic + "\ttransactionId = " + transactionId +
                "\tboostrapServers = " + boostrapServers);
        init();

    }

    private void init() {

        final Properties props = new Properties();
        props.put("bootstrap.servers", boostrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // Idempotence = exactly once semantics between producer and partition
        props.put("enable.idempotence", true);
        props.put("transactional.id", transactionId);
        this.producer = new KafkaProducer<>(props);

        System.out.println(console + "Source initialized");
    }

    @Override
    public void run() {

        System.out.println(console + " running");
        final List<String> topics = Collections.singletonList(outTopic);
        /*file reader*/
        File file = new File("source.txt");
        String[] words;
        String line;
        final Random r = new Random();
        producer.initTransactions();
        System.out.println(console + " init transactions");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file),
                StandardCharsets.UTF_8))) {
            while ((line = br.readLine()) != null) {
                words = line.split("\\s");
                for (String word : words) {
                    final String topic = topics.get(r.nextInt(topics.size()));
                    producer.beginTransaction();
                    System.out.println(console + " beginnned");
                    final String key = "Key" + r.nextInt(5);

                    System.out.println(console + " Topic : " + topic + "\t" + //
                            "Key: " + key + "\t" + //
                            "Value: " + word);

                    final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, word);
                    producer.send(record);
                    producer.commitTransaction();
                    try {

                        Thread.sleep(waitBetweenMsgs);
                    } catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            System.out.println(console + "We can't recover from these exceptions, so our only option is to close the producer and exit.");
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println(console + "producer aborts the transaction. Try again.");
            producer.abortTransaction();
        }
        System.out.println(console + "source closing");
        producer.close();



        /*final int numMessages = 50;
        List<String> alpha = new ArrayList<>(Arrays.asList("A","B","C","D","E","F","G","H","I","L","M","N","O","P","Q",
                "R","S","T","U","V","Z",
                "A","B","C","D","E","F","G","H","I","L","M","N","O","P","Q","R","S","T","U","V","Z",
                "A","B","C","D","E","F","G","H","I","L","M","N","O","P","Q","R","S","T","U","V","Z"));
        List<String> phrase = new ArrayList<>(Arrays.asList("Life", "is", "what", "happens", "when", "you're", "busy",
                "making", "other", "plans"));
        final Random r = new Random();
        // This must be called before any method that involves transactions
        producer.initTransactions();
        System.out.println(console+" init transactions");

        try {
            for (int i = 0; i < numMessages; i++) {
                final String topic = topics.get(r.nextInt(topics.size()));
                producer.beginTransaction();
                System.out.println(console+" beginnned");
                final String key = "Key" + r.nextInt(5);
                final String value = phrase.get(i);

                System.out.println(console+" Topic : " + topic + "\t" + //
                        "Key: " + key + "\t" + //
                        "Value: " + value);


                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record);
                producer.commitTransaction();
                try {

                    Thread.sleep(waitBetweenMsgs);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

            }

        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            System.out.println(console+"We can't recover from these exceptions, so our only option is to close the producer and exit.");
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println(console+"producer aborts the transaction. Try again.");
            producer.abortTransaction();
        }
        System.out.println(console+"source closing");
        producer.close();*/

    }
}

