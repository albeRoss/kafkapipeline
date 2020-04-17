package org.middleware.project;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.middleware.project.topology.AtomicForwarder;
import org.middleware.project.topology.Source;

public class TopologyBuilder {

    private String inTopic;
    private String outTopic;
    private int pipelineLength;
    private String boostrapServer;
    private final static short replication_factor = 2;

    private Properties loadEnvProperties(String fileName) {
        Properties prop = new Properties();
        try {

            InputStream inputStream = TopologyBuilder.class.getClassLoader().getResourceAsStream(fileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + fileName + "' not found in the classpath");
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    private static final void err() {
        System.out.println("Usage: Topology builder <numConsumers>");
        System.exit(1);
    }

    public void build_stage(int pos) throws InterruptedException {

        int outTopic_pos = pos + 1;
        String group = "group_" + pos;

        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");

        String stage_processors_str = global_prop.getProperty("processors.at." + pos);
        int stage_processors = Integer.parseInt(stage_processors_str);

        String function_type = global_prop.getProperty("stage.at." + pos);

        props.put("inTopic", "topic_" + pos);
        NewTopic topic = new NewTopic("topic_" + outTopic_pos, stage_processors, replication_factor);
        Properties adminConf = this.loadEnvProperties("adminClient.properties");
        AdminClient adminClient = KafkaAdminClient.create(adminConf);
        adminClient.createTopics(Collections.singleton(topic));
        adminClient.close();
        props.put("outTopic", "topic_" + outTopic_pos);
        props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        props.put("group.id", group);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("transactionId", "atomic_forwarder_" + group + "_pos_" + pos + "_stage_" + function_type + "transactional_id");
        props.put("stage_function", function_type);

        // create processors
        final ExecutorService executor = Executors.newFixedThreadPool(stage_processors);
        for (int i = 0; i < stage_processors; i++) {
            executor.submit(new AtomicForwarder(props));
        }
        executor.shutdown();
        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        }

    }


    public void build_source() {
        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");
        int partitions = Integer.parseInt(global_prop.getProperty("processors.at.1"));
        NewTopic topic = new NewTopic("topic_" + 1, partitions, replication_factor);
        Properties adminConf = this.loadEnvProperties("adminClient.properties");
        AdminClient adminClient = KafkaAdminClient.create(adminConf);
        adminClient.createTopics(Collections.singleton(topic));
        adminClient.close();
        props.put("outTopic", "topic_" + 1);
        props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));

        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("transactionId", "source_transactional_id");

        //create source producer

        new Source(props);


    }

    public void generate_server_properties(int num_brokers) {
        for (int i = 0; i < num_brokers; i++) {
            try (OutputStream output = new FileOutputStream("server"+i+".properties")) {

                Properties prop = new Properties();

                // set the properties value
                prop.setProperty("listeners","" );
                prop.setProperty("listeners", "PLAINTEXT://:909"+ (i+2));
                prop.setProperty("advertised.listeners", "PLAINTEXT://:909"+ (i+2));
                prop.setProperty("log.dirs", "/tmp/kafka-logs"+i);

                // save properties to project root folder
                String path = this.getClass().getResource("/").getPath();
                prop.store(output, null);

                System.out.println(prop);

            } catch (IOException io) {
                io.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {

        /* read properties from configuration file*/
        System.out.println("Reading configuration file..");
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        Properties prop = topologyBuilder.loadEnvProperties("config.properties");
        final int pipeline_length = new Integer(prop.getProperty("pipeline.length"));

        //generate props to be read by start_cluster.sh
        topologyBuilder.generate_server_properties(pipeline_length);


        //build stages
        for (int i = 1; i < pipeline_length; i++) {

            try {
                topologyBuilder.build_stage(i);
                System.out.println("Stage at position ( " + i + " ) built");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //build source

        topologyBuilder.build_source();
        System.out.println("Source Built");




        /*final String[] pipeline_functions = new String[pipeline_length];
        final int[] stage_processors = new
        for (int i = 0; i < pipeline_length; i++) {
            pipeline_functions[i] = prop.getProperty("stage.at."+ i);
        }*/


        // create the topics
        // create the


    }

}
