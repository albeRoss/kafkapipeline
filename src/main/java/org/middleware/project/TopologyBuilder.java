package org.middleware.project;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import javafx.util.Pair;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.topology.AtomicStage;
import org.middleware.project.topology.Sink;
import org.middleware.project.topology.Source;
import org.middleware.project.topology.StatefulAtomicStage;
import org.middleware.project.utils.Bash_runner;

public class TopologyBuilder {

    private static boolean isLocal = false;

    public Properties loadEnvProperties(String fileName) {
        /**
         * utility function:
         * loads properties from ./resources folder
         * @param : relative path to .properties
         */
        Properties prop = new Properties();
        try {

            InputStream inputStream = new FileInputStream("resources/"+fileName);
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
       /**
        * utiility function:
        * exits
        */
       System.out.println("exiting");
        System.exit(1);
    }

    /*public Properties build_stage(int pos) {

        int outTopic_pos = pos + 1;
        String group = "group_" + pos;
        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");
        String stage_processors_str = global_prop.getProperty("processors.at." + (pos));
        assert stage_processors_str != null;
        int stage_processors = Integer.parseInt(stage_processors_str);
        // String function_type = global_prop.getProperty("stage.at." + pos);

        props.put("inTopic", "topic_" + pos);
        props.put("outTopic", "topic_" + outTopic_pos);
        if(isLocal) props.put("bootstrap.servers", "localhost:9092");
        else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers")); //fixme public ip

        props.put("group.id", group);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("stage_processors", stage_processors_str);
        // props.put("stage_function", function_type);

        System.out.println("Stage [" + pos + "] created with " + stage_processors + " processors");

        return props;
    }*/

    public ArrayList<Properties> buildStage(int pos)  {

        int outTopic_pos = pos + 1;

        Properties global_prop = this.loadEnvProperties("config.properties");
        String stage_processors_str = global_prop.getProperty("processors.at." + (pos));
        String stage_function = global_prop.getProperty("function.at."+pos);
        assert stage_function != null;
        assert stage_processors_str != null;
        int stage_processors = Integer.parseInt(stage_processors_str);
        Random r = new Random();
        int low = 4;
        ArrayList<Properties> ls_properties = new ArrayList<>();
        System.out.println("searching a match for:"+stage_function);
        switch (stage_function){
            case "flatmap":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    props.put("simulateCrash",Integer.toString(0)); // change here to simulate crash
                    props.put("id",Integer.toString(i));
                    props.put("type","stateless");
                    props.put("group.id", "group_" + pos);
                    props.put("inTopic", "topic_" + pos);
                    props.put("outTopic", "topic_" + outTopic_pos);
                    props.put("function","flatmap");
                    if (isLocal) props.put("bootstrap.servers", "localhost:9092");
                    else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
                    ls_properties.add(props);

                }
                break;
            case "map":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    if(i%2==0){ // set false not to simulate a crash
                        props.put("simulateCrash",Integer.toString(r.nextInt(10-low) + low)); // change here to simulate crash

                    }else props.put("simulateCrash",Integer.toString(0));
                    props.put("id",Integer.toString(i));
                    props.put("type","stateless");
                    props.put("group.id", "group_" + pos);
                    props.put("inTopic", "topic_" + pos);
                    props.put("outTopic", "topic_" + outTopic_pos);
                    props.put("function","map");
                    if (isLocal) props.put("bootstrap.servers", "localhost:9092");
                    else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
                    ls_properties.add(props);

                }
                break;
            case "filter":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    if(false){ // set false not to simulate a crash
                        props.put("simulateCrash",Integer.toString(r.nextInt(10-low) + low)); // change here to simulate crash

                    }else props.put("simulateCrash",Integer.toString(0));
                    props.put("id",Integer.toString(i));
                    props.put("type","stateless");
                    props.put("group.id", "group_" + pos);
                    props.put("inTopic", "topic_" + pos);
                    props.put("outTopic", "topic_" + outTopic_pos);
                    props.put("function","filter");
                    if (isLocal) props.put("bootstrap.servers", "localhost:9092");
                    else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
                    ls_properties.add(props);

                }
                break;
            case "windowaggregate":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    if(i%2==0){ // set false not to simulate a crash
                        props.put("simulateCrash",Integer.toString(r.nextInt(10-low) + low)); // change here to simulate crash

                    }else props.put("simulateCrash",Integer.toString(0));

                    props.put("id",Integer.toString(i));
                    props.put("crash","between"); // possible values: before | after | between
                    props.put("type", "stateful");
                    props.put("group.id", "group_" + pos);
                    props.put("inTopic", "topic_" + pos);
                    props.put("outTopic", "topic_" + outTopic_pos);
                    props.put("function", "windowaggregate");
                    if (isLocal) props.put("bootstrap.servers", "localhost:9092");
                    else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
                    ls_properties.add(props);

                }
                break;
            default:
                System.out.println("the function you provided at stage "+pos+" doesn't match any function implemented");
                System.out.println("retry");
                err();


        }
        System.out.println("Stage [" + pos + "] created with " + stage_processors + " processors");
        return ls_properties;
    }


    public Properties build_source() {

        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");
        props.put("outTopic", "topic_" + 1);
        props.put("type","source");
        if(isLocal) props.put("bootstrap.servers", "localhost:9092");
        else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        props.put("transactionId", "source_transactional_id");
        System.out.println("Source configured");

        return props;

    }


  /*  private void generate_server_properties(int num_brokers) {
        //FIXME add zookeeper.connect
        String path = System.getProperty("user.dir")+"/";

        if(isLocal) path = System.getProperty("user.dir") + "/../kafka_2.12-2.3.1/config/";

        for (int i = 0; i < num_brokers; i++) {
            File f = new File(path + "server" + i + ".properties"); //fixme folder for each server
            try (OutputStream output = new FileOutputStream(f)) {

                //Properties prop = new Properties();
                // set the properties value
                Properties prop_default = this.loadEnvProperties("server.properties");

                if(isLocal){
                    prop_default.setProperty("listeners", "PLAINTEXT://:909" + (2 + (i * 2)));

                }else{
                    //FIXME if is first server then zookeeper is localhost. Else it is private ip of first machine of cluster

                    //prop_default.setProperty("zookeeper.connect","")
                    prop_default.setProperty("listeners", "PLAINTEXT://"+loadEnvProperties("config.properties")
                            .getProperty("bootstrap.servers.internal")+":909" + (2 + (i * 2)));
                    prop_default.setProperty("advertised.listeners",
                            "PLAINTEXT://"+loadEnvProperties("config.properties").getProperty("bootstrap.servers"));

                }
                if(isLocal) prop_default.setProperty("log.dirs", "/tmp/kafka-logs" + i);
                else prop_default.setProperty("log.dirs", "/tmp/kafka-logs" + i);
                // save properties to project kafka /config folder
                prop_default.store(output, null);

            } catch (IOException io) {
                io.printStackTrace();
            }

        }
        System.out.println("server.properties generated");
    }*/

    static void appendUsingPrintWriter(String filePath, String text) {
        /**
         * utility function:
         * appends to file
         * @param: path to file to write into
         * @param: text to append
         * @return: void
         */
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


   /* private void transferFiles(String pipelineLength, String server){
        *//**
         * transfer files for a broker to start to one server, the files are composed of a list of server.properties and a .sh to launch kafka
         * (and eventually zookeeper).
         **//*
        System.out.println("uploading configuration files to cloud..");
        try {
            String[] cmdArray = new String[3];
            cmdArray[0] = "./transferFiles.sh";
            cmdArray[1] = pipelineLength;
            cmdArray[2] = server;
            //File sh_path = new File(System.getProperty("user.dir")+"/../kafka_2.12-2.3.1/");
            //Process chmod = Runtime.getRuntime().exec("chmod +x transferFiles.sh", null, sh_path);
            //chmod.waitFor();
            Process proc = Runtime.getRuntime().exec(cmdArray,null,null);
            proc.waitFor();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Uploaded");
    }*/

    /*private void generate_sh(int num_brokers, final ArrayList<Integer> partitions_array) {
        String sh_path = System.getProperty("user.dir")+ "/start_kafka_cluster.sh";

        if(isLocal) sh_path = System.getProperty("user.dir") + "/../kafka_2.12-2.3.1/start_kafka_cluster.sh";

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
            if(isLocal)  TopologyBuilder.appendUsingPrintWriter(sh_path,
                    "./bin/kafka-server-start.sh config/server" + i + ".properties &\n");
            else TopologyBuilder.appendUsingPrintWriter(sh_path,
                    "KAFKA_HEAP_OPTS=\"-Xmx512M -Xms512M\" ./bin/kafka-server-start.sh config/server" + i + ".properties &\n");
        }

        // generate topics:
        if(isLocal){
            for (int i = 0; i < partitions_array.size(); i++) {
                TopologyBuilder.appendUsingPrintWriter(sh_path,
                        "./bin/kafka-topics.sh " +
                                "--create " +
                                "--bootstrap-server localhost:9092 " +
                                "--replication-factor " + replication_factor + " " +
                                "--partitions " + partitions_array.get(i).toString() + " " +
                                "--topic topic_" + (i + 1) + " &\n");
            }

        }else{
            for (int i = 0; i < partitions_array.size(); i++) {
                TopologyBuilder.appendUsingPrintWriter(sh_path,
                        "./bin/kafka-topics.sh " +
                                "--create " +
                                "--bootstrap-server "+loadEnvProperties("config.properties")
                                .getProperty("bootstrap.servers.internal")+":9092 " + //fixme add private hostname retrieved from adminClient properties
                                "--replication-factor " + replication_factor + " " +
                                "--partitions " + partitions_array.get(i).toString() + " " +
                                "--topic topic_" + (i + 1) + " &\n");
            }
        }


        System.out.println("topology deployment sh generated");
    }*/

    /*private void generate_cluster_sh(){
        int num_servers = Integer.parseInt(loadEnvProperties("config.properties").getProperty("cluster_servers"));
        for (int i = 1; i < num_servers; i++) {
            String sh_path = System.getProperty("user.dir")+ "/start_kafka_cluster"+i+".sh";

        }
    }*/

    public ArrayList<Properties> build_stages(){

        Properties global_prop = this.loadEnvProperties("config.properties");
        int pipeline_length = Integer.parseInt(global_prop.getProperty("pipeline.length"));

        //build stages properties
        ArrayList<Properties> lst_stage_props = new ArrayList<>();

        for (int i = 0; i < pipeline_length; i++) {

            lst_stage_props.addAll(this.buildStage(i + 1));
        }
        return lst_stage_props;

    };

    /*private void start_kafka_cluster() {
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
    }*/

    public Properties build_sink() {


        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");
        int pipelineLength = Integer.parseInt(global_prop.getProperty("pipeline.length"));
        props.put("type","sink");
        props.put("inTopic", "topic_" + (pipelineLength + 1));
        if(isLocal) props.put("bootstrap.servers", "localhost:9092");
        else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        System.out.println("Sink configured");
        return props;

    }

    public static void main(String[] args) {


       /* *//* read topology of pipeline from configuration file*//*
        System.out.println("Reading configuration file..");
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Properties prop = topologyBuilder.loadEnvProperties("config.properties");
        int pipeline_length = new Integer(prop.getProperty("pipeline.length"));
        replication_factor = new Short(prop.getProperty("replication.factor"));
        System.out.println("pipeline length: " + pipeline_length);


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

        //check replication factor to be consistent with cluster size
        if (replication_factor > final_cluster_size) {
            System.out.println("Stop at configuration. Final cluster size calculated is less than replication factor.\n"+
            "Either increase number of processors per stage or decrease replication factor.\n"+
            "be aware that decreasing replication factor to less than 2 makes the cluster not reliable ");
            err();
        }
        System.out.println("total number of thread needed is: " + num_thread_pipeline);
        System.out.println("bottleneck of pipeline: " + final_cluster_size);

        //generate props to be read by start_cluster.sh
        // here the number of broker is determined by the biggest consumer group in the pipeline
        topologyBuilder.generate_server_properties(final_cluster_size);

        //once done, transfer this files to the server
        if(!isLocal) topologyBuilder.transferFiles(prop.getProperty("pipeline.length"),topologyBuilder.loadEnvProperties("config.properties")
                .getProperty("bootstrap.servers.external"));//FIXME


        // generate .sh file
        topologyBuilder.generate_sh(final_cluster_size, topic_partitions);

        //once done wait for client to give continue

        System.out.println("properties and .sh uploaded to server, launch it in server if is not local. Press c if you want to continue x to exit");
        Scanner input = new Scanner(System.in);
        String x = input.nextLine();
        if(x.equals("x")) err();
        else if (x.equals("c")) {}
            else {
            System.out.println("wrong typing retry");
            err();
        }

        //fixme isLocal check else do not launch

        // launch script deploy cluster and create topics
        if(isLocal) topologyBuilder.start_kafka_cluster();

        //build source
        Properties propSource = topologyBuilder.build_source();

        //build sink
        Properties propSink = topologyBuilder.build_sink();


        //build stages properties
        ArrayList<Properties> lst_stage_props = new ArrayList<>();

        for (int i = 0; i < pipeline_length; i++) {

            lst_stage_props.add(topologyBuilder.build_stage(i + 1));
        }

        //retrieve defined pipeline
        PipelineFunctions pipelineFunctions = PipelineFunctions.pipeline_1;

        if (PipelineFunctions.pipeline_1.getProcessors().size() != pipeline_length) {
            throw new IllegalArgumentException("user defined pipeline is inconsistent with pipeline_length in config.prop");
        }
        List<StageProcessor> stages = pipelineFunctions.getProcessors();

        // executor of pipeline (processors+source+sink)
        final ExecutorService executor_stage = Executors.newFixedThreadPool(num_thread_pipeline + 2);
        try {

            for (int j = 0; j < lst_stage_props.size(); j++) {
                int processors = Integer.parseInt(lst_stage_props.get(j).getProperty("stage_processors"));

                if (stages.get(j).getClass().getSimpleName().matches("WindowedAggregateProcessor")) {
                    for (int i = 0; i < processors; i++) {

                        // here you can simulate a crash of a stateful stage
                        CompletableFuture.runAsync(new StatefulAtomicStage(lst_stage_props.get(j), i,
                                stages.get(j), 5),executor_stage).exceptionally(throwable -> {
                                    //here we handle restart of crashed processors

                            DB dbc = DBMaker.fileDB("crashedThreads.db").fileMmapEnableIfSupported().make();
                            System.out.println("stateful processor restart");
                            ConcurrentMap<Integer, Pair<Integer, String>> mapc =
                                    dbc.hashMap("crashedThreads", Serializer.INTEGER, Serializer.JAVA).createOrOpen();
                            System.out.println("size of current crashedThreadmap is: "+ mapc.size());
                            for (Map.Entry<Integer, Pair<Integer, String>> crashed : mapc.entrySet()) {
                                System.out.println("restarting processor\t id : " + crashed.getKey() + "\t stagePos: "
                                        + crashed.getValue().getKey() + " : " + crashed.getValue().getValue());
                                if (crashed.getValue().getValue().equals("stateful")) {
                                    int id = crashed.getKey();
                                    int pos = crashed.getValue().getKey();
                                    CompletableFuture.runAsync(new StatefulAtomicStage(lst_stage_props.get(pos-1), id,
                                            stages.get(pos-1), 0),Executors.newFixedThreadPool(1));
                                    mapc.remove(crashed.getKey(), crashed.getValue());

                                } else {
                                    System.out.println("there is a queue of failed processes, scrolling");
                                }

                            }
                            dbc.close();
                            System.out.println("scrolled every entry of crashed threads");
                            return null;
                        });
                    }
                } else {
                    for (int i = 0; i < processors; i++) {
                        // here you can simulate a crash of a stateless stage
                        CompletableFuture.runAsync(new AtomicStage(lst_stage_props.get(j), i, stages.get(j),
                                0),executor_stage).exceptionally(throwable -> {
                            //here we handle restart of crashed processors
                            DB dbc = DBMaker.fileDB("crashedThreads.db").fileMmapEnableIfSupported().make();
                            System.out.println("stateless processor restart");
                            ConcurrentMap<Integer, Pair<Integer, String>> mapc =
                                    dbc.hashMap("crashedThreads", Serializer.INTEGER, Serializer.JAVA).createOrOpen();
                            //if (mapc.isEmpty()) System.out.println("error: map empty");
                            System.out.println("size of current crashedThreadmap is: "+ mapc.size());
                            for (Map.Entry<Integer, Pair<Integer, String>> crashed : mapc.entrySet()) {
                                if (crashed.getValue().getValue().equals("stateless")) {
                                    int id = crashed.getKey();
                                    int pos = crashed.getValue().getKey();
                                    System.out.println("removing reference from crashedThreads");
                                    System.out.println("restarting processor\t id : " + id + "\t stagePos: "
                                            + pos);
                                    CompletableFuture.runAsync(new AtomicStage(lst_stage_props.get(pos-1), id,
                                            stages.get(pos-1), 0),Executors.newFixedThreadPool(1));
                                    mapc.remove(id, crashed.getValue());
                                    System.out.println("main thread : crashed thread served");
                                    break;

                                } else {
                                    System.out.println("there is a queue of failed processes, scrolling");
                                }

                            }
                            dbc.close();
                            System.out.println("scrolled every entry of crashed threads");
                            return null;
                        });

                    }

                }

            }
            executor_stage.submit(new Source(propSource));
            executor_stage.submit(new Sink(propSink));

            executor_stage.shutdown();
            while (!executor_stage.awaitTermination(10, TimeUnit.SECONDS)) {
            }

        } catch (InterruptedException e) {
            e.printStackTrace();

        }*/

    }


}
