package org.middleware.project;

import java.io.*;
import java.util.*;

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

    private static final void err() {
        /**
         * utiility function:
         * exits
         */
        System.out.println("exiting");
        System.exit(1);
    }

    public ArrayList<Properties> buildStage(int pos) {

        int outTopic_pos = pos + 1;

        Properties global_prop = this.loadEnvProperties("config.properties");
        String stage_processors_str = global_prop.getProperty("processors.at." + (pos));
        String stage_function = global_prop.getProperty("function.at." + pos);
        assert stage_function != null;
        assert stage_processors_str != null;
        int stage_processors = Integer.parseInt(stage_processors_str);
        Random r = new Random();
        int low = 4;
        ArrayList<Properties> ls_properties = new ArrayList<>();
        System.out.println("searching a match for:" + stage_function);
        switch (stage_function) {
            case "flatmap":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    props.put("simulateCrash", Integer.toString(0)); // change here to simulate crash
                    props.put("id", Integer.toString(i));
                    props.put("type", "stateless");
                    props.put("group.id", "group_" + pos);
                    props.put("inTopic", "topic_" + pos);
                    props.put("outTopic", "topic_" + outTopic_pos);
                    props.put("function", "flatmap");
                    if (isLocal) props.put("bootstrap.servers", "localhost:9092");
                    else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
                    ls_properties.add(props);

                }
                break;
            case "map":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    if (i % 2 == 0) { // set false not to simulate a crash
                        props.put("simulateCrash", Integer.toString(r.nextInt(10 - low) + low)); // change here to simulate crash

                    } else props.put("simulateCrash", Integer.toString(0));
                    props.put("id", Integer.toString(i));
                    props.put("type", "stateless");
                    props.put("group.id", "group_" + pos);
                    props.put("inTopic", "topic_" + pos);
                    props.put("outTopic", "topic_" + outTopic_pos);
                    props.put("function", "map");
                    if (isLocal) props.put("bootstrap.servers", "localhost:9092");
                    else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
                    ls_properties.add(props);

                }
                break;
            case "filter":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    if (false) { // set false not to simulate a crash
                        props.put("simulateCrash", Integer.toString(r.nextInt(10 - low) + low)); // change here to simulate crash

                    } else props.put("simulateCrash", Integer.toString(0));
                    props.put("id", Integer.toString(i));
                    props.put("type", "stateless");
                    props.put("group.id", "group_" + pos);
                    props.put("inTopic", "topic_" + pos);
                    props.put("outTopic", "topic_" + outTopic_pos);
                    props.put("function", "filter");
                    if (isLocal) props.put("bootstrap.servers", "localhost:9092");
                    else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
                    ls_properties.add(props);

                }
                break;
            case "windowaggregate":
                for (int i = 0; i < stage_processors; i++) {
                    Properties props = new Properties();
                    if (i % 2 == 0) { // set false not to simulate a crash
                        props.put("simulateCrash", Integer.toString(r.nextInt(10 - low) + low)); // change here to simulate crash

                    } else props.put("simulateCrash", Integer.toString(0));

                    props.put("id", Integer.toString(i));
                    props.put("crash", "between"); // possible values: before | after | between
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
                System.out.println("the function you provided at stage " + pos + " doesn't match any function implemented");
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
        props.put("type", "source");
        if (isLocal) props.put("bootstrap.servers", "localhost:9092");
        else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        props.put("transactionId", "source_transactional_id");
        System.out.println("Source configured");

        return props;

    }

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

    public ArrayList<Properties> build_stages() {

        Properties global_prop = this.loadEnvProperties("config.properties");
        int pipeline_length = Integer.parseInt(global_prop.getProperty("pipeline.length"));

        //build stages properties
        ArrayList<Properties> lst_stage_props = new ArrayList<>();

        for (int i = 0; i < pipeline_length; i++) {

            lst_stage_props.addAll(this.buildStage(i + 1));
        }
        return lst_stage_props;

    }

    public Properties build_sink() {


        Properties props = new Properties();
        Properties global_prop = this.loadEnvProperties("config.properties");
        int pipelineLength = Integer.parseInt(global_prop.getProperty("pipeline.length"));
        props.put("type", "sink");
        props.put("inTopic", "topic_" + (pipelineLength + 1));
        if (isLocal) props.put("bootstrap.servers", "localhost:9092");
        else props.put("bootstrap.servers", global_prop.getProperty("bootstrap.servers"));
        System.out.println("Sink configured");
        return props;

    }


}
