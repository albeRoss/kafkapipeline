package org.middleware.project.topology;

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
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.middleware.project.Processors.FlatMapProcessor;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.Processors.WindowedAggregateProcessor;
import org.middleware.project.functions.WindowedAggregate;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class StatefulAtomicStage extends AtomicStage {

    private TopicPartition partition;
    private WindowedAggregateProcessor winStageProcessor;
    private long processedOffset;

    public StatefulAtomicStage(Properties properties, int id, StageProcessor stageProcessor) {
        super(properties, id, stageProcessor);
        winStageProcessor = (WindowedAggregateProcessor) stageProcessor;


    }

    protected DB openDBSession() {
        return DBMaker
                .fileDB(this.group + id + ".db")
                .transactionEnable()
                .make();
    }

    protected Long getOffset(DB db) {
        HTreeMap<Integer, Long> processedOffsetsMap = db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
        return processedOffsetsMap.get(this.id);
    }

    protected HTreeMap<Integer, Long>  getOffsetsMap(DB db) {
        return db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
    }


    protected HTreeMap<String, List<String>> getWindows(DB db) {
        return db.hashMap("windows", Serializer.STRING, Serializer.JAVA).createOrOpen();
    }

    @Override
    public void init() {

        /* Stage Local State retrieval*/

        DB db = this.openDBSession();
        ConcurrentMap<String, List<String>> windows = getWindows(db);

        //in case first start: initialize windows as empty
        //in case restart: retrieve topic partition current window
        winStageProcessor.setWindows(windows);

        // HTreeMap<Integer, Long> processedOffsetsMap = db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
        // processedOffsetsMap.;
        // db.commit();
        // db.close();

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", boostrapServers);
        consumerProps.put("group.id", group);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("isolation.level", "read_committed");
        consumerProps.put("enable.auto.commit", "false");

        this.consumer = new KafkaConsumer<>(consumerProps);

        TopicPartition partition = new TopicPartition(inTopic, id);
        this.consumer.assign(Collections.singleton(partition));

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

        HashMap<String, String> processed = winStageProcessor.process(record);

        if (!processed.isEmpty()) {
            for (String key : processed.keySet()) {
                String value = processed.get(key);
                this.producer.send(new ProducerRecord<>(outTopic, key, value));
            }
        }

    }

    @Override
    public void run() {
        DB db = openDBSession();
        try {
            this.producer.initTransactions();

            while (running) {

                long lastConsumedOffset = getOffset(db);
                consumer.seek(this.partition, lastConsumedOffset);

                ConsumerRecords<String, String> records = this.consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                this.producer.beginTransaction();

                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("[GROUP : " + group + " ] " + "[" + inTopic + "] " +
                            "[FORWARDER : " + id + " ] : " +
                            "Partition: " + record.partition() + "\t" + //
                            "Offset: " + record.offset() + "\t" + //
                            "Key: " + record.key() + "\t" + //
                            "Value: " + record.value());

                    process(record);

                    // save last offset processed
                    processedOffset = record.offset();
                    HTreeMap <Integer, Long> processedOffsetMap = getOffsetsMap(db);
                    processedOffsetMap.put(id,processedOffset);

                }
                db.commit();
                db.close();

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
            db.close();

        }


    }

    public void shutdown() {
        running = false;
    }
}

