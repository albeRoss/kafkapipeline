package org.middleware.project;

import com.sun.prism.paint.Color;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.Processors.WindowedAggregateProcessor;
import org.middleware.project.functions.WindowedAggregate;
import org.middleware.project.topology.AtomicStage;
import org.middleware.project.utils.ConsoleColors;
import org.middleware.project.utils.Pair;
import org.middleware.project.utils.Utils;


import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Hello world!
 */
public class App {


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

    public void prova(HTreeMap<String, List<String>> map) {

        map.put("A", Arrays.asList("1", "2"));


    }

    public void executeScript() throws IOException, InterruptedException {
        File sh_path = new File(System.getProperty("user.dir") + "/../kafka_2.12-2.3.1/");
        Process p = Runtime.getRuntime().exec("/bin/bash -c ./start_kafka_cluster.sh", null, sh_path);
        p.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));


        String line = "";
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        line = "";
        while ((line = errorReader.readLine()) != null) {
            System.out.println(line);
        }


    }

    protected DB openDBSession() {
        return DBMaker
                .fileDB("prova.db")
                .transactionEnable()
                .make();
    }

    protected Long getOffset(DB db) {
        HTreeMap<Integer, Long> processedOffsetsMap = db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
        return processedOffsetsMap.get(0);
    }

    protected HTreeMap<String, List<String>> getWindows(DB db) {
        return db.hashMap("windows", Serializer.STRING, Serializer.JAVA).createOrOpen();
    }

    public int[] distribute(int servers, int processors) {
        int[] distribution = new int[servers];
        for (int i = 0; i < processors; i++) {
            distribution[i % servers] += 1;

        }
        return distribution;
    }

    public static void main(String[] args) throws IOException, InterruptedException {


        App app = new App();

        System.out.println("failure!");
        DB dbc = DBMaker.fileDB("crashedThreads.db").fileMmapEnableIfSupported().make();
        HTreeMap<Integer, Pair<Integer, String>> mapc = dbc.hashMap("crashedThreads", Serializer.INTEGER, Serializer.JAVA).createOrOpen();
        System.out.println("failure!");
        //we need the id of the processor and the stage position

        mapc.put(1, new Pair<>(1, "stateful"));
        System.out.println("failure!");
        dbc.close();
        System.out.println("ok!");


    }
}

