package org.middleware.project;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.middleware.project.Processors.FilterProcessor;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.topology.AtomicStage;
import org.middleware.project.topology.Sink;
import org.middleware.project.topology.Source;
import org.middleware.project.utils.Bash_runner;

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

    private Properties build_stage(int pos) throws InterruptedException {

        int outTopic_pos = pos + 1;
        String group = "group_" + pos;
        //System.out.println("[STAGE : " + pos + " ]");
        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");

        String stage_processors_str = global_prop.getProperty("processors.at." + (pos));
        int stage_processors = Integer.parseInt(stage_processors_str);
        String function_type = global_prop.getProperty("stage.at." + pos);

        props.put("inTopic", "topic_" + pos);
        /*NewTopic topic = new NewTopic("topic_" + outTopic_pos, stage_processors, replication_factor);
        Properties adminConf = this.loadEnvProperties("adminClient.properties");
        AdminClient adminClient = KafkaAdminClient.create(adminConf);
        adminClient.createTopics(Collections.singleton(topic));
        adminClient.close(Duration.ofSeconds(4));
        System.out.println("New topic created: " + "topic_" + outTopic_pos);
        System.out.println("    partitions: " + stage_processors);
        System.out.println("    replication_factor: " + replication_factor);*/
        props.put("outTopic", "topic_" + outTopic_pos);
        props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        props.put("group.id", group);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("stage_processors", stage_processors_str);
        props.put("stage_function", function_type);
        // System.out.println("(topic_" + pos+") --> [STAGE@"+pos+"] --> "+"(topic_" + outTopic_pos+")");
        // create processors
        /*final ExecutorService executor_stage = Executors.newFixedThreadPool(stage_processors+1);
        executor_stage.submit(new Source(propSource));
        executor_stage.shutdown();
        while (!executor_stage.awaitTermination(10, TimeUnit.SECONDS)) {
        }*/
        System.out.println("Stage [" + pos + "] created with " + stage_processors + " processors");
        return props;


    }


    private Properties build_source() {
        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");
        props.put("outTopic", "topic_" + 1);
        props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("transactionId", "source_transactional_id");
        System.out.println("Source built");

        return props;


    }


    private void generate_server_properties(int num_brokers) {
        String path = System.getProperty("user.dir") + "/../kafka_2.12-2.3.1/config/";

        for (int i = 0; i < num_brokers; i++) {
            File f = new File(path + "server" + i + ".properties");
            try (OutputStream output = new FileOutputStream(f)) {

                //Properties prop = new Properties();
                // set the properties value
                Properties prop_default = this.loadEnvProperties("server.properties");
                prop_default.setProperty("listeners", "PLAINTEXT://:909" + (i * 2));
                //prop_default.setProperty("advertised.listeners", "PLAINTEXT://909" + (i*2));
                prop_default.setProperty("log.dirs", "/tmp/kafka-logs" + i);
                // save properties to project kafka /config folder
                prop_default.store(output, null);

            } catch (IOException io) {
                io.printStackTrace();
            }

        }
        System.out.println("server.properties generated");
    }

    private static void appendUsingPrintWriter(String filePath, String text) {
        File file = new File(filePath);
        FileWriter fr = null;
        BufferedWriter br = null;
        PrintWriter pr = null;
        try {
            // to append to file, you need to initialize FileWriter using below constructor
            fr = new FileWriter(file, true);
            br = new BufferedWriter(fr);
            pr = new PrintWriter(br);
            pr.println(text);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pr.close();
                br.close();
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void generate_sh(int num_brokers, final ArrayList<Integer> partitions_array) {

        String sh_path = System.getProperty("user.dir") + "/../kafka_2.12-2.3.1/start_kafka_cluster.sh";
        File script = new File(sh_path);
        if (script.exists()) {
            script.delete();
        }
        try {
            script.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TopologyBuilder.appendUsingPrintWriter(sh_path, "#!/usr/bin/env bash\n");
        TopologyBuilder.appendUsingPrintWriter(sh_path,
                "./bin/zookeeper-server-start.sh config/zookeeper.properties &\n");
        for (int i = 0; i < num_brokers; i++) {
            TopologyBuilder.appendUsingPrintWriter(sh_path,
                    "./bin/kafka-server-start.sh config/server" + i + ".properties &\n");
        }

        // generate topics:
        for (int i = 0; i < partitions_array.size(); i++) {
            TopologyBuilder.appendUsingPrintWriter(sh_path,
                    "./bin/kafka-topics.sh " +
                            "--create " +
                            "--bootstrap-server localhost:9092 " +
                            "--replication-factor " + replication_factor + " " +
                            "--partitions " + partitions_array.get(i).toString() + " " +
                            "--topic topic_" + (i+1) + " &\n");
        }

        System.out.println("topology deployment sh generated");
    }

    private void start_kafka_cluster() {
        try {
            final ExecutorService executor = Executors.newFixedThreadPool(2);
            executor.submit(new Bash_runner());
            executor.shutdown();
            System.out.println("[Admin] : kafka cluster and Zookeeper launched.");
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        /* read topology of pipeline from configuration file*/
        System.out.println("Reading configuration file..");
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Properties prop = topologyBuilder.loadEnvProperties("config.properties");
        final int pipeline_length = new Integer(prop.getProperty("pipeline.length"));

        // get max cluster size needed and the total number of thread for each forwarder
        int final_cluster_size = 1;
        int num_thread_pipeline = 0;
        ArrayList<Integer> topic_partitions = new ArrayList<>();
        for (int i = 1; i < pipeline_length + 1; i++) {

            int cluster_size = new Integer(prop.getProperty("processors.at." + i));
            num_thread_pipeline += cluster_size;
            topic_partitions.add(cluster_size);
            if (cluster_size > final_cluster_size) final_cluster_size = cluster_size;

        }
        System.out.println("total number of thread needed is: "+num_thread_pipeline);
        System.out.println("bottleneck of pipeline: "+final_cluster_size);

        //generate props to be read by start_cluster.sh
        // here the number of broker is determined by the biggest consumer group in the pipeline
        topologyBuilder.generate_server_properties(final_cluster_size);

        // create .sh file
        topologyBuilder.generate_sh(final_cluster_size, topic_partitions);

        // launch script deploy cluster and create topics
        topologyBuilder.start_kafka_cluster();


        //build source
        Properties propSource = topologyBuilder.build_source();

        //build sink
        Properties propSink = topologyBuilder.build_sink(pipeline_length);


        /*//launch source
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            executor.submit(new Source(propSource));
            executor.shutdown();
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/


        //build stages
        ArrayList<Properties> lst_stage_props = new ArrayList<>();
        for (int i = 0; i < pipeline_length; i++) {

            try {
                lst_stage_props.add(topologyBuilder.build_stage(i + 1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /*//FIXME filter stage:
        StageProcessor stage = new FilterProcessor((String k, String v) -> {
            if (k.hashCode() % 2 == 1) {
                return true;
            } else {
                return false;
            }
        });*/
        PipelineFunctions pipelineFunctions = PipelineFunctions.pipeline;
        List<StageProcessor> stages = pipelineFunctions.getProcessors();

        // executor of pipeline (processors+source+sink)
        final ExecutorService executor_stage = Executors.newFixedThreadPool(num_thread_pipeline + 2);

        try {
            for (int j = 0; j < lst_stage_props.size(); j++) {
                int processors = Integer.parseInt(lst_stage_props.get(j).getProperty("stage_processors"));

                for (int i = 0; i < processors; i++) {

                    executor_stage.submit(new AtomicStage(lst_stage_props.get(j), i,stages.get(j))); // fixme we need to look at each fun
                }

            }
            executor_stage.submit(new Source(propSource));
            executor_stage.submit(new Sink())
            executor_stage.shutdown();
            while (!executor_stage.awaitTermination(10, TimeUnit.SECONDS)) {
            }


        } catch (InterruptedException e) {
            e.printStackTrace();
            //executor_stage.submit(new Source(propSource));
            /*try {
                final ExecutorService executor_source = Executors.newFixedThreadPool(1);
                executor_source.submit(new Source(propSource));
                executor_source.shutdown();
                while (!executor_stage.awaitTermination(10, TimeUnit.SECONDS)) {
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/



        /*final String[] pipeline_functions = new String[pipeline_length];
        final int[] stage_processors = new
        for (int i = 0; i < pipeline_length; i++) {
            pipeline_functions[i] = prop.getProperty("stage.at."+ i);
        }*/


        }

    }

    private Properties build_sink(int pipelineLength) {
        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");
        props.put("inTopic", "topic_" + pipelineLength);
        props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        System.out.println("Sink built");
        return props;

    }
}
