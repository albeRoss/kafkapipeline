package org.middleware.project.topology;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import org.middleware.project.utils.ConsoleColors;
import org.middleware.project.utils.Pair;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class StatefulAtomicProcessor extends StatelessAtomicProcessor {

    private final String crash;
    private TopicPartition partition;
    private WindowedAggregateProcessor winStageProcessor;
    private int simulateCrash;
    private String console;

    public StatefulAtomicProcessor(Properties properties, StageProcessor stageProcessor) {
        this.simulateCrash = Integer.parseInt(properties.getProperty("simulateCrash"));
        this.crash = properties.getProperty("crash");
        if (simulateCrash == 0) this.simulateCrash = Integer.MAX_VALUE;
        else {
            System.out.println("this processor will crash: \t" + this.id + "\t" + group);
        }
        this.group = properties.getProperty("group.id");
        this.inTopic = properties.getProperty("inTopic");
        this.outTopic = properties.getProperty("outTopic");
        this.boostrapServers = properties.getProperty("bootstrap.servers");
        this.id = Integer.parseInt(properties.getProperty("id"));
        this.transactionId = "atomic_forwarder_" + properties.getProperty("group.id") + "_transactional_id_" + id;
        this.stage_function = stageProcessor.getClass().getSimpleName();

        running = true;
        this.console = ConsoleColors.YELLOW_BRIGHT + "[ " + this.stage_function.toUpperCase() + " : " + this.id + "\t GROUP : " + group + " ] ";
        System.out.println(console + "\t inTopic = " + inTopic
                + "\t outTopic = " + outTopic + "\t boostrapServers = " + boostrapServers);

        this.stageProcessor = (WindowedAggregateProcessor) stageProcessor;
        winStageProcessor = ((WindowedAggregateProcessor) stageProcessor).clone();


        this.pos = Integer.parseInt(group.substring(6));
        winStageProcessor.setPos(pos);
        System.out.println("before init");
        init();


    }

    private DB openDBSession() {
        return DBMaker
                .fileDB(this.group + id + ".db")
                .transactionEnable()
                .make();
    }

    private long getOffset(DB db) {
        HTreeMap<Integer, Long> processedOffsetsMap = db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
        long res = processedOffsetsMap.get(this.id);
        return res;
    }

    private void crash() {
        System.out.println(console + "failure!");
        DB dbc = DBMaker.fileDB("crashedThreads.db").fileMmapEnableIfSupported().make();
        ConcurrentMap<Integer, Pair<Integer, String>> mapc = dbc.hashMap("crashedThreads", Serializer.INTEGER, Serializer.JAVA).createOrOpen();
        System.out.println(console + "failure!");
        //we need the id of the processor and the stage position

        mapc.put(id, new Pair<>(pos, "stateful"));
        System.out.println(console + "failure!");
        dbc.close();
        System.out.println(console + "failure!");
        //producer.abortTransaction();
        consumer.close();
        producer.close();
        running = false;
        System.out.println(console + "failure!");
        throw new RuntimeException("[failure] : " + this.stageProcessor.getClass().getSimpleName());

    }

    private HTreeMap<Integer, Long> getOffsetsMap(DB db) {
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
        System.out.println("after init");
        DB db = this.openDBSession();
        ConcurrentMap<String, List<String>> windows = getWindows(db);

        ConcurrentMap<Integer, Long> offsetsMap = getOffsetsMap(db);
        ConcurrentMap<String, List<String>> oldSlidedValues = getOldSlidedValues(db);
        System.out.println("after retrieving state");
        //first time
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
        consumerProps.put("max.poll.records", "1");

        this.consumer = new KafkaConsumer<>(consumerProps);

        this.partition = new TopicPartition(inTopic, id);
        this.consumer.assign(Collections.singleton(partition));
        System.out.println("after assigning partition");

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
        System.out.println(console + "Stateful processor initialized");

    }

    @Override
    public void process(final ConsumerRecord<String, String> record) {

        HashMap<String, String> processed = winStageProcessor.process(record);

        if (!processed.isEmpty()) {
            for (String key : processed.keySet()) {
                String value = processed.get(key);

                //if we don't need to prerestart just send as always
                if (!prerestart) {
                    this.producer.send(new ProducerRecord<>(outTopic, key, value), new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println(console + "The offset of the record we just sent is: " + metadata.offset());
                            }
                        }
                    });
                } else {
                    // if we need to recover last message do not send current record, save only and then set prerestart to false
                    prerestart = false;
                }

            }
        }

    }

    public void restart(DB db) {


        long lastLocalConsumedOffset = getOffset(db);

        //need to compare last consumed offset with last local committed offset
        if (lastLocalConsumedOffset != 0) {


            long nextOffsetTobeFetched = (long) consumer.endOffsets(Collections.singleton(partition)).values().toArray()[0];
            long lastOffsetConsumed = nextOffsetTobeFetched - 2;
            System.out.println(console + "last kafka fetched offset was: " + lastOffsetConsumed);

            //retrieve the last committed kafka offset of intopic and partition during transaction
            long kafkaOffset = consumer.committed(partition).offset();
            System.out.println(console + "last kafka committed offset is :" + (kafkaOffset - 1));
            //if necessary rollback
            if ((kafkaOffset > lastOffsetConsumed) && (lastOffsetConsumed > lastLocalConsumedOffset)) {

                System.out.println(console + "Consumer last committed offset is: " + kafkaOffset +
                        " but last consumed Offset is: " + lastOffsetConsumed +
                        "\n.We should poll one record to sync state, without forwarding the record ");

                consumer.seek(this.partition, lastOffsetConsumed);
                prerestart = true;

            } else {
                prerestart = false;
            }
        }


        //if we are in sync with kafkaOffset ( kafkaoffset == 2+lastconsumedOffset ) then we have not to do anything
        //consumer.seek(this.partition, lastLocalConsumedOffset);
    }

    @Override
    public void run() {

        DB db = openDBSession();

        ConcurrentMap<String, List<String>> windows = getWindows(db);
        ConcurrentMap<String, List<String>> oldSlidedValues = getOldSlidedValues(db);
        winStageProcessor.setWindows(windows);
        winStageProcessor.setOldSlidedValues(oldSlidedValues);

        try {

            //prerestart routine
            restart(db);

            // after initTransactions returns any transactions started by another instance of a producer
            // with the same transactional.id would have been closed and fenced off
            this.producer.initTransactions();

            while (running) {

                // max.poll.records is set to 1



                ConsumerRecords<String, String> records = this.consumer.poll(Duration.of(1, ChronoUnit.MINUTES));
                this.producer.beginTransaction(); // request to the coordinator and await response
                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println(console +
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

                /* CRASH BLOCK CODE */

                if (simulateCrash > 0 && crash.equals("before")) {
                    simulateCrash--;
                } else {
                    //db.rollback();
                   if(crash.equals("before")){
                       db.close();
                       crash();
                   }

                }
                Thread.sleep(100);

                /*-----------------*/

                // The producer manually commits the outputs for the consumer within the
                // transaction
                final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                for (final TopicPartition partition : records.partitions()) {
                    final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }

                //TRY CRASH

                this.producer.sendOffsetsToTransaction(map, group);
                this.producer.commitTransaction(); //  the offsets and the output records will be committed as an atomic unit

                //TRY CRASH
                if (simulateCrash > 0 && crash.equals("between")) {
                    simulateCrash--;
                } else {
                    //db.rollback();
                    if(crash.equals("between")){
                        db.close();
                        crash();
                    }

                }

                db.commit();

                if (simulateCrash > 0 && crash.equals("after")) {
                    simulateCrash--;
                } else {
                    //db.rollback();
                    if(crash.equals("after")){
                        db.close();
                        crash();
                    }

                }
                //TRY CRASH


            }

            db.close();
            this.consumer.close();
            this.producer.close();
        } catch (InterruptedException e) {
            System.out.println(console + "AtomicStatefulStageinterrupted:  possible rollback needed");
            //this is needed for interruptions occurred before commit.
            // for after commit interruption: the rollback is handled at restart (first part of run)
            db.rollback();
            e.printStackTrace();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            System.out.println(console + "We can't recover from these exceptions, so our only option is to close the producer and exit.");
            producer.close();
            consumer.close();
            running = false;
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println(console + "aborts the transaction. Try again.");
            producer.abortTransaction();
            // if it aborts
            db.rollback();
            // retry comes for free
        }

    }

    public void shutdown() {
        running = false;
    }
}

