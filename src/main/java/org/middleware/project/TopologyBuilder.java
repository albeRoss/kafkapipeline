package org.middleware.project;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.clients.admin.NewTopic;

public class TopologyBuilder {

    private String inTopic;
    private String outTopic;
    private int pipelineLength;
    private String boostrapServer;

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

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Properties prop = topologyBuilder.loadEnvProperties("config.properties");
        System.out.println(prop.getProperty("bootstrap.servers"));
        System.out.println(prop.stringPropertyNames());



    }

}
