package org.middleware.project.topology;

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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.Processors.WindowedAggregateProcessor;
import org.middleware.project.utils.Tuple;


import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class StatefulAtomicStage extends AtomicStage {

    private TopicPartition partition;
    private WindowedAggregateProcessor winStageProcessor;
    private int simulateCrash;

    public StatefulAtomicStage(Properties properties, int id, StageProcessor stageProcessor, int simulateCrash) {
        if(simulateCrash == 0) this.simulateCrash = Integer.MAX_VALUE;
        else this.simulateCrash = simulateCrash;
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
        winStageProcessor = (WindowedAggregateProcessor) stageProcessor;
        init();


    }

    private DB openDBSession() {
        return DBMaker
                .fileDB(this.group + id + ".db")
                .transactionEnable()
                .make();
    }

    private long getOffset(DB db) {
        System.out.println("getting");
        HTreeMap<Integer, Long> processedOffsetsMap = db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
        long res = processedOffsetsMap.get(this.id);
        System.out.println("got offset: " +res);
        return res;
    }

    private void crash(){

        DB dbc = DBMaker.fileDB("crashedThreads.db").transactionEnable().make();
        ConcurrentMap mapc = dbc.hashMap("crashedThreads").createOrOpen();

        //we need the id of the processor and the stage position

        mapc.put(id, new Pair<>(pos,"stateful"));
        dbc.commit();
        dbc.close();
        consumer.close();
        producer.close();
        running = false;
        throw new RuntimeException("[failure] : "+ this.stageProcessor.getClass().getSimpleName());

    }
    private HTreeMap<Integer, Long>  getOffsetsMap(DB db) {
        return db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
    }


    private HTreeMap<String, List<String>> getWindows(DB db) {
        return db.hashMap("windows", Serializer.STRING, Serializer.JAVA).createOrOpen();
    }

    private HTreeMap<String, List<String>> getOldSlidedValues(DB db) {
        return db.hashMap("oldSlidedValues", Serializer.STRING, Serializer.JAVA).createOrOpen();
    }


    @Override
    public void init() {

        /* Stage Local State retrieval*/



        DB db = this.openDBSession();
        ConcurrentMap<String, List<String>> windows = getWindows(db);
        ConcurrentMap<Integer,Long> offsetsMap = getOffsetsMap(db);
        ConcurrentMap<String, List<String>> oldSlidedValues = getOldSlidedValues(db);


        offsetsMap.putIfAbsent(this.id, (long) 0);
        //in case first start: initialize windows as empty
        //in case restart: retrieve topic partition current window
        winStageProcessor.setWindows(windows);
        winStageProcessor.setOldSlidedValues(oldSlidedValues);


        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", boostrapServers);
        consumerProps.put("group.id", group);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("isolation.level", "read_committed");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("max.poll.records","1");

        this.consumer = new KafkaConsumer<>(consumerProps);

        this.partition = new TopicPartition(inTopic, id);
        this.consumer.assign(Collections.singleton(partition));

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", boostrapServers);
        producerProps.put("group.id", group);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("transactional.id", transactionId);
        producerProps.put("enable.idempotence", true);

        this.producer = new KafkaProducer<>(producerProps);
        db.commit();
        db.close();
        System.out.println("Stateful stage initialized");

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

        System.out.println(1);
        DB db = openDBSession();

        ConcurrentMap<String, List<String>> windows = getWindows(db);
        ConcurrentMap<String,List<String>> oldSlidedValues = getOldSlidedValues(db);
        winStageProcessor.setWindows(windows);
        winStageProcessor.setOldSlidedValues(oldSlidedValues);

        System.out.println(2);
        try {
            // after initTransactions returns any transactions started by another instance of a producer
            // with the same transactional.id would have been closed and fenced off
            this.producer.initTransactions();

            long lastLocalConsumedOffset = getOffset(db);

            //need to compare last consumed offset with last local committed offset
            long kafkaOffset = ( long ) consumer.endOffsets(Collections.singleton(partition)).values().toArray()[0];

            //if necessary rollback
            if(kafkaOffset < lastLocalConsumedOffset){

                System.out.println("Consumer last offset is: "+ kafkaOffset +
                        " but our last local saved consumed Offset is: "+ lastLocalConsumedOffset);
                //rollback last kafka-uncommitted message
                winStageProcessor.rollback();
                lastLocalConsumedOffset = kafkaOffset;
                System.out.println("Resort to old committed values, and proceed from there");
            }
            consumer.seek(this.partition, lastLocalConsumedOffset+1);

            while (running){
                // max.poll.records is set to 1
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.of(1, ChronoUnit.MINUTES));
                this.producer.beginTransaction();
                System.out.println(3);
                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("[GROUP : " + group + " ] " + "[" + inTopic + "] " +
                            "[FORWARDER : " + id + " ] : " +
                            "Partition: " + record.partition() + "\t" + //
                            "Offset: " + record.offset() + "\t" + //
                            "Key: " + record.key() + "\t" + //
                            "Value: " + record.value());

                    process(record);

                    // save last offset processed
                    long processedOffset = record.offset();
                    HTreeMap<Integer, Long> processedOffsetMap = getOffsetsMap(db);
                    processedOffsetMap.put(id, processedOffset);

                }

                // The producer manually commits the outputs for the consumer within the
                // transaction
                final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                for (final TopicPartition partition : records.partitions()) {
                    final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }
                db.commit();

               /* if (simulateCrash > 0){
                    simulateCrash--;
                }else crash();*/

                                        // possible crash here we need to rollback on previous committed state (only one slide)
                                         // NB the poll gets 1 record at a time only.
                this.producer.sendOffsetsToTransaction(map, group);
                this.producer.commitTransaction(); //  the offsets and the output records will be committed as an atomic unit

                if (simulateCrash > 0){
                    simulateCrash--;
                }else {
                    db.close();
                    crash();
                }
                Thread.sleep(100);
            }

            db.close();
            this.consumer.close();
            this.producer.close();
        }catch (InterruptedException e) {
            System.out.println("AtomicStatefulStageinterrupted:  possible rollback needed");
            //this is needed for interruptions occurred before commit.
            // for after commit interruption: the rollback is handled at restart (first part of run)
            db.rollback();
            e.printStackTrace();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            System.out.println("    We can't recover from these exceptions, so our only option is to close the producer and exit.");
            producer.close();
            consumer.close();
            running = false;
        } catch (KafkaException e ) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println("     aborts the transaction. Try again.");
            producer.abortTransaction();
            // if it aborts
            db.rollback();
            // retry comes for free
        } /*catch (RuntimeException e){
            System.out.println("crash restart thread");
            e.printStackTrace();

        } finally {
            System.out.println("    finally closing atomic forwarder at group: " + group);
            consumer.close();
            producer.close();
            running = false;
            db.close();

        }*/

    }

    public void shutdown() {
        running = false;
    }
}

