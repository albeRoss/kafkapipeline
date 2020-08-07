package org.middleware.project;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.middleware.project.utils.Utils;

import java.io.*;
import java.util.*;

public class ClusterLauncher {

    /**
     * This method depends <code> get_broker_ips </code> . Reads cluster.json and extracts the number of servers
     * instantiated on AWS
     * @author Alberto Rossettini
     * @return <code> int </code> : the number of servers instanciated
     * @throws IOException    in case of cluster.json doesn't exists
     * @throws ParseException if <code>JSONParser cannot </code>
     */
    public int get_cluster_size() throws IOException, ParseException {

        JSONParser parser = new JSONParser();
        JSONArray servers = new JSONArray();
        JSONArray data = (JSONArray) parser.parse(
                new FileReader("cluster.json"));
        for (Object o : data
        ) {
            servers = (JSONArray) o;

        }
        return servers.size();

    }

    /**
     * utility function to load .properties
     * @param fileName the name of the file to be loaded
     * @return <code> Properties </code> the requested properties
     */

    public static Properties loadEnvProperties(String fileName) {
        Properties prop = new Properties();
        try {

            InputStream inputStream = new FileInputStream("resources/" + fileName);
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

    /**
     * adds IP addresses of launched instances in config.properties
     *
     * @throws IOException
     * @throws ParseException
     */
    public void prepareConfigurationCluster() throws IOException, ParseException {

        JSONParser parser = new JSONParser();

        JSONArray data = (JSONArray) parser.parse(
                new FileReader("cluster.json"));
        Properties props;
        String path = System.getProperty("user.dir") + "/";
        int i = 0;
        for (Object o : data
        ) {
            JSONArray server = (JSONArray) o;
            for (Object item : server
            ) {
                JSONObject aServer = (JSONObject) item;
                props = loadEnvProperties("config.properties");

                String oldDNS = props.getProperty("bootstrap.servers.external");
                String oldBS = props.getProperty("bootstrap.servers");

                //is the first node of cluster
                if (oldBS.isEmpty()) {

                    String newDNS = (String) aServer.get("DNS"); //for sending
                    String newBS = aServer.get("DNS") + ":9092"; // advertised listeners

                    props.setProperty("zookeeper.connect.internal", aServer.get("IP") + ":2181"); // for other servers
                    props.setProperty("bootstrap.servers.external", newDNS);
                    props.setProperty("bootstrap.servers", newBS); // for producers and consumers

                } else {
                    String newDNS = oldDNS.concat("," + aServer.get("DNS"));
                    String newBS = oldBS.concat("," + aServer.get("DNS") + ":9092");
                    props.setProperty("bootstrap.servers.external", newDNS);
                    props.setProperty("bootstrap.servers", newBS);
                }

                File f = new File(System.getProperty("user.dir") + "/resources/config.properties");
                try (OutputStream output = new FileOutputStream(f)) {
                    props.store(output, null);
                } catch (IOException io) {
                    io.printStackTrace();
                }

                File f_p = new File(path + "/server" + i + "/server0.properties");
                f_p.getParentFile().mkdirs();
                Properties prop_default = loadEnvProperties("server.properties");

                try (OutputStream output = new FileOutputStream(f_p)) {

                    String listeners = addPlainText(aServer.get("IP") + ":9092");
                    String adListeners = addPlainText(aServer.get("DNS") + ":9092");

                    prop_default.setProperty("zookeeper.connect", props.getProperty("zookeeper.connect.internal"));
                    prop_default.setProperty("listeners", listeners);
                    prop_default.setProperty("advertised.listeners", adListeners);
                    prop_default.setProperty("log.dirs", "/tmp/kafka-logs");

                    // save properties in the folder folder
                    prop_default.store(output, null);

                } catch (IOException io) {
                    io.printStackTrace();
                }
                i++;

            }
        }

        System.out.println("Endpoints servers added");
    }

    /**
     * generates a folder for each server that contains server0.properties and only for the first server a
     * script with
     * that start broker and zookeeper and also generate the topics for the pipeline
     *
     * @param num_brokers number of brokers speficied in configuration files
     * @deprecated  Replaced by
     * @see {@link #prepareConfigurationCluster()}  }
     */
    private void generate_server_folders(int num_brokers) {

        String path = System.getProperty("user.dir") + "/";

        for (int i = 0; i < num_brokers; i++) {

            // set the properties value
            Properties prop_default = loadEnvProperties("server.properties");
            Properties prop_config = loadEnvProperties("config.properties");

            File f = new File(path + "/server" + i + "/server0.properties");
            f.getParentFile().mkdirs();

            try (OutputStream output = new FileOutputStream(f)) {

                String listeners = addPlainText(prop_config.getProperty("bootstrap.servers.internal"));
                String adListeners = addPlainText(prop_config.getProperty("bootstrap.servers"));

                prop_default.setProperty("zookeeper.connect", prop_config.getProperty("zookeeper.connect.internal"));
                prop_default.setProperty("listeners", listeners);
                prop_default.setProperty("advertised.listeners", adListeners);
                prop_default.setProperty("log.dirs", "/tmp/kafka-logs");

                // save properties in the folder folder
                prop_default.store(output, null);

            } catch (IOException io) {
                io.printStackTrace();
            }

        }
        System.out.println("Server.properties generated");
    }

    /**
     * utility function
     * adds protocol reference for ips correct interpretation
     * @param listOfServers commma separated strings corresponding to the ips of EC2 instances
     * @return <code> String </code>  the modified string
     */
    public String addPlainText(final String listOfServers) {
        List<String> array = Arrays.asList(listOfServers.split(","));
        for (String item : array) {
            array.set(array.indexOf(item), "PLAINTEXT://".concat(item));
        }
        String res = String.join(",", array);
        return res;
    }

    /**
     *
     * @return
     */
    public int get_number_of_processors() {
        int finalsize = 0;
        Properties prop = loadEnvProperties("config.properties");
        int pipeline_length = Integer.parseInt(prop.getProperty("pipeline.length"));
        for (int i = 1; i < pipeline_length + 1; i++) {

            assert prop.getProperty("processors.at." + i) != null;
            int cluster_size = new Integer(prop.getProperty("processors.at." + i));
            finalsize += cluster_size;

        }
        return finalsize;
    }

    /**
     * returns a list containing integers numbers that correspond to the number of partition for each topic.
     * The order of the element in the list reflects to the order of the topics in the pipeline
     * @param pipeline_length
     * @return
     */
    public ArrayList<Integer> getListSizeOfPartitions(int pipeline_length) {

        Properties prop = loadEnvProperties("config.properties");
        ArrayList<Integer> topic_partitions = new ArrayList<>();
        for (int i = 1; i < pipeline_length + 1; i++) {

            int cluster_size = new Integer(prop.getProperty("processors.at." + i));
            topic_partitions.add(cluster_size);

        }
        return topic_partitions;

    }

    /**
     * Generates <i>start_kafka_broker.sh</i> and <i>create_topics.sh</i>. The former its a modified version on the same
     * script that run on each server: this one contains commands to start and distribute all the brokers with requested
     * configuration. The latter creates the topics that are used throughout the pipeline
     *
     * @throws IOException
     */
    private void generate_sh_first_server() {

        String sh_path = System.getProperty("user.dir") + "/server0/start_kafka_broker.sh";
        String sh_create_topics = System.getProperty("user.dir") + "/create_topics.sh";

        //retrieve replication factor and pipeline.length from config
        String replication_factor = loadEnvProperties("config.properties").getProperty("replication.factor");
        int pipelineLength = Integer.parseInt(loadEnvProperties("config.properties").getProperty("pipeline.length"));


        ArrayList<Integer> partitions = getListSizeOfPartitions(pipelineLength);

        File script = new File(sh_path);
        File topics = new File(sh_create_topics);
        script.getParentFile().mkdirs();
        topics.getParentFile().mkdirs();
        if (script.exists()) {
            script.delete();
        }
        try {
            script.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (topics.exists()) {
            topics.delete();
        }
        try {
            topics.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        TopologyBuilder.appendUsingPrintWriter(sh_path, "#!/usr/bin/env bash\n");
        TopologyBuilder.appendUsingPrintWriter(sh_path,
                "./bin/zookeeper-server-start.sh -daemon config/zookeeper.properties &\n");

        TopologyBuilder.appendUsingPrintWriter(sh_path,
                "KAFKA_HEAP_OPTS=\"-Xmx512M -Xms512M\" ./bin/kafka-server-start.sh -daemon config/server0.properties &\n");

        //create topics for the pipeline
        for (int i = 0; i < partitions.size(); i++) {
            TopologyBuilder.appendUsingPrintWriter(sh_create_topics,
                    "../kafka_2.12-2.3.1/bin/kafka-topics.sh " +
                            "--create " +
                            "--bootstrap-server " + loadEnvProperties("config.properties")
                            .getProperty("bootstrap.servers") + " " +
                            "--replication-factor " + replication_factor + " " +
                            "--partitions " + partitions.get(i).toString() + " " +
                            "--topic topic_" + (i + 1) + " &\n");
        }

        TopologyBuilder.appendUsingPrintWriter(sh_create_topics,
                "../kafka_2.12-2.3.1/bin/kafka-topics.sh " +
                        "--create " +
                        "--bootstrap-server " + loadEnvProperties("config.properties")
                        .getProperty("bootstrap.servers") + " " +
                        "--replication-factor " + replication_factor + " " +
                        "--partitions " + 1 + " " +
                        "--topic topic_" + (partitions.size() + 1) + " &\n");
        System.out.println(".sh for first node generated");
    }

    /**
     * Utility function
     * Accesses local resources and wipe up fields in .properties files
     * @throws IOException
     */
    public void clean_properties() {


        // cleaning config.properties
        try (OutputStream output = new FileOutputStream(System.getProperty("user.dir") + "/resources/config_start.properties")) {
            Properties config = loadEnvProperties("config.properties");
            config.setProperty("pipeline.length", config.getProperty("pipeline.length"));
            for (int i = 1; i < Integer.parseInt(config.getProperty("pipeline.length")) + 1; i++) {
                config.setProperty("processors.at." + i, config.getProperty("processors.at." + i));
            }
            config.setProperty("replication.factor", config.getProperty("replication.factor"));
            config.setProperty("bootstrap.servers.internal", "");
            config.setProperty("zookeeper.connect.internal", "");
            config.setProperty("bootstrap.servers", "");
            config.setProperty("bootstrap.servers.external", "");
            config.setProperty("zookeeper.connect.external", "");

            config.store(output, null);


        } catch (IOException e) {
            e.printStackTrace();
        }

        renameconfig();
        System.out.println("config.properties cleaned");

    }

    /**
     * Deletes processors configurations generated at previous runs
     * Delegates to <code>remove_processors_properties.sh</code>
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     */
    public void clean_cluster_properties() {

        try {

            int size_cluster = get_cluster_size();

            String[] cmdArray = new String[2];
            cmdArray[0] = "./remove_processor_properties.sh";
            cmdArray[1] = Integer.toString(size_cluster);
            Process proc = Runtime.getRuntime().exec(cmdArray, null, null);
            proc.waitFor();
        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Utility function
     * renames configuration file <i>config_start.properties</i> to <i>config.properties</i>
     * @throws IOException
     */
    public void renameconfig() {
        try {
            File f1 = new File(System.getProperty("user.dir") + "/resources/config_start.properties");
            File f2 = new File(System.getProperty("user.dir") + "/resources/config.properties");
            boolean b = f1.renameTo(f2);
            if (!b) throw new IOException("rename file: unsuccessful");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @return ArrayList : the list of public ips of EC2 instances
     */
    public ArrayList<String> getListOfServersPublicIps() {
        String list_instances = loadEnvProperties("config.properties").getProperty("bootstrap.servers.external");
        return new ArrayList(Arrays.asList(list_instances.split(",")));
    }

    /**
     * transfer files for a kafka broker to each instance, the files are composed of a  server.properties and
     * a .sh to launch kafka with zookeeper to a random instance
     */
    public void transferFiles() {
        ArrayList<String> listOfServers = getListOfServersPublicIps();
        try {

            for (int i = 0; i < listOfServers.size(); i++) {
                String[] cmdArray = new String[3];
                cmdArray[0] = "./transferProperties.sh";
                cmdArray[1] = Integer.toString(i);
                cmdArray[2] = listOfServers.get(i);
                Process proc = Runtime.getRuntime().exec(cmdArray, null, null);
                proc.waitFor();
            }


        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Configuration files uploaded on AWS EC2 instances:\n" + listOfServers);
    }

    /**
     * Delegates to <code> launch_kafka_cluster.sh </code>
     * @throws IOException
     */
    public void launchKafkaOnMachines() {

        ArrayList<String> listOfServers = getListOfServersPublicIps();
        try {

            for (int i = 0; i < listOfServers.size(); i++) { // reverse order because the first would fail to create the topics
                String[] cmdArray = new String[2];
                cmdArray[0] = "./launch_kafka_cluster.sh";
                cmdArray[1] = listOfServers.get(i);
                Runtime.getRuntime().exec(cmdArray, null, null);
                //proc.waitFor(); // we must not wait
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Kafka launched on AWS EC2 instances:\n" + listOfServers);
    }

    /**
     *
     * Delegates to <code>create_topics.sh</code>
     * @throws InterruptedException
     * @throws IOException
     */
    public void create_topics() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            Process chmod = Runtime.getRuntime().exec("chmod +x ./create_topics.sh", null,
                    new File(System.getProperty("user.dir")));
            chmod.waitFor();
            String[] cmdArray = new String[1];
            cmdArray[0] = "./create_topics.sh";
            Runtime.getRuntime().exec(cmdArray, null, null);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("topics created");

    }

    /**
     * Launches as many EC2 instances as requested
     * @link README.md EC2 default type
     * Delegates to <code>launch_instances.sh</code>
     * @throws IOException
     * @throws InterruptedException
     */
    public void launch_instances() {
        System.out.println("How many instances? Insert a number, x to exit");
        Scanner input = new Scanner(System.in);
        String x = input.nextLine();
        if (x.equals("x")) err();
        System.out.println("You selected: " + x + " instances. Do you want to continue? Press c to continue x to exit");
        Scanner input_2 = new Scanner(System.in);
        String x_2 = input_2.nextLine();
        if (x_2.equals("x")) err();
        else if (x_2.equals("c")) {
            String[] cmdArray = new String[2];
            cmdArray[0] = "./launch_instances.sh";
            cmdArray[1] = x;
            try {
                Process proc = Runtime.getRuntime().exec(cmdArray, null, null);
                proc.waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        } else {
            System.out.println("wrong typing retry");
            err();
        }
    }

    /**
     * Retrieves servers ips and saves them in json format into <i>cluster.json</i>
     * Delegates to <code> get_brokers_ips.sh </code>
     * @throws IOException
     * @throws InterruptedException
     */
    public void get_brokers_ips() {
        String[] cmdArray = new String[1];
        cmdArray[0] = "./get_brokers_ips.sh";
        try {
            Process proc = Runtime.getRuntime().exec(cmdArray, null, null);
            proc.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("instances launched");
    }

    /**
     * Uploads configuration files and runnable to the servers
     * Delegates to <code> transfer_processors_files.sh </code>
     * @throws IOException
     * @throws InterruptedException
     */
    public void transfer_processors_files() {

        ArrayList<String> listOfServers = getListOfServersPublicIps();
        try {

            for (int i = 0; i < listOfServers.size(); i++) {
                String[] cmdArray = new String[3];
                cmdArray[0] = "./transfer_processors_files.sh";
                //cmdArray[1] = "processor";
                cmdArray[1] = listOfServers.get(i);
                cmdArray[2] = Integer.toString(i);
                Process proc = Runtime.getRuntime().exec(cmdArray, null, null);
                proc.waitFor();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Configuration files for processors uploaded on AWS EC2 instances:\n" + listOfServers);

    }

    /**
     * Launches the application server-side
     * Delegates to <code> run_processors_on_servers.sh </code>
     * @throws IOException
     * @throws InterruptedException
     */
    public void launch_processors() {

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ArrayList<String> listOfServers = getListOfServersPublicIps();
        try {
            for (int i = 0; i < listOfServers.size(); i++) {
                String[] cmdArray = new String[2];
                cmdArray[0] = "./run_processors_on_servers.sh";
                cmdArray[1] = listOfServers.get(i);
                Process proc = Runtime.getRuntime().exec(cmdArray, null, null);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("processors running on AWS EC2 instances:\n" + listOfServers);


    }

    /**
     * Generates properties for each processor in the pipeline, the source and the sink
     *
     * @throws IOException
     * @throws ParseException
     */
    public void generate_properties_processors() throws IOException, ParseException {

        /* cleaning first */
        clean_cluster_properties();

        //generate properties
        TopologyBuilder tp = new TopologyBuilder();
        Properties source = tp.build_source();
        Properties sink = tp.build_sink();
        ArrayList<Properties> properties_stages = tp.build_stages();
        //properties_stages.add(source);

        int servers = this.get_cluster_size();
        int[] distribution = Utils.distribute(servers, properties_stages.size());

        System.out.println("distribution: " + Arrays.toString(distribution));

        int past = 0;
        for (int i = 0; i < distribution.length; i++) {

            if (i > 0) past += distribution[i - 1];

            for (int j = 0; j < distribution[i]; j++) {

                File f = new File(System.getProperty("user.dir") + "/server" + i + "/processor" + j +
                        ".properties");
                try (OutputStream output = new FileOutputStream(f)) {

                    properties_stages.get(past + j).store(output, null);

                } catch (IOException io) {
                    io.printStackTrace();
                }
            }
        }

        File f = new File(System.getProperty("user.dir") + "/sink.properties");
        try (OutputStream output = new FileOutputStream(f)) {

            sink.store(output, null);

        } catch (IOException io) {
            io.printStackTrace();
        }
        File f_source = new File(System.getProperty("user.dir") + "/source.properties");
        try (OutputStream output = new FileOutputStream(f_source)) {

            source.store(output, null);

        } catch (IOException io) {
            io.printStackTrace();
        }
    }

    /**
     * Utility function
     * checks if replication factor in config.properties is consistent with number of instances requested and the
     * instances can sustain in a scalable manner the processors of the pipeline. Exits if the replication factor is
     * lower than the number of instances
     * @throws ParseException
     * @throws IOException
     */
    private void checks() {

        //check replication factor < number of servers
        int replication_factor = Integer.parseInt(loadEnvProperties("config.properties")
                .getProperty("replication.factor"));
        try {
            int cluster_size = get_cluster_size();

            if (replication_factor > cluster_size) {
                System.out.println("Stop at configuration. Final cluster size calculated is less than replication " +
                        "factor.\n" +
                        "Either increase number of servers or decrease replication factor.\n" +
                        "be aware that decreasing replication factor to less than 2 makes the cluster not reliable ");
                err();
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        try {
            //check dimension of cluster so that physical cores > processors
            String[] cmdArray = new String[1];
            cmdArray[0] = "./run_processors_on_servers.sh";
            Process p = Runtime.getRuntime().exec(cmdArray, null, null);
            BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";

            while ((line = b.readLine()) != null) {
                int cores = Integer.parseInt(line);
                int num_processors = get_number_of_processors();
                if (cores < num_processors) {
                    System.out.println("[WARNING] \t you selected a cluster with " + cores + " total number of cores " +
                            "and " +
                            +num_processors + " total number of processors. Select another configuration or continue" +
                            " and" +
                            "take advantage of aws hyperthreading capabilities.");
                    System.out.println("Press c to continue, x to exit");
                }
            }

            b.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static final void err() {
        System.out.println("exiting");
        System.exit(1);
    }

    /**
     * mainclass local JAR
     * Entrypoint for the pipeline to be launched
     * @param args (optional)
     * @throws IOException
     * @throws ParseException
     */
    public static void main(String[] args) throws IOException, ParseException {

        ClusterLauncher clusterLauncher = new ClusterLauncher();

        System.out.println("Do you want to launch new instances? Press s to skip, c to run the new instances x to " +
                "exit");
        Scanner input = new Scanner(System.in);
        String x = input.nextLine();
        if (x.equals("x")) err();
        else if (x.equals("c")) {
            clusterLauncher.launch_instances();
        } else if (x.equals("s")) {
        } else {
            System.out.println("wrong typing retry");
            err();
        }
        System.out.println("getting brokers ips");
        clusterLauncher.get_brokers_ips();

        clusterLauncher.clean_properties();

        clusterLauncher.prepareConfigurationCluster();

        clusterLauncher.checks();

        clusterLauncher.generate_sh_first_server();

        /*create properties for processors*/
        clusterLauncher.generate_properties_processors();

        clusterLauncher.transfer_processors_files();

        clusterLauncher.transferFiles();

        clusterLauncher.launchKafkaOnMachines();

        clusterLauncher.create_topics();

        clusterLauncher.launch_processors();

    }


}
