package org.middleware.project;


import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.topology.*;
import org.middleware.project.utils.Pair;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProcessorStarter {

    /**
     * mainclass server JAR
     *
     * @param args
     */
    public static void main(String[] args) {

        try {
            final ExecutorService executor_stage = Executors.newFixedThreadPool(2);
            InputStream inputStream = new FileInputStream(args[0]);
            Properties properties = new Properties();
            properties.load(inputStream);
            StageProcessor function = null;
            if (properties.getProperty("type").equals("source")) {
                CompletableFuture.runAsync(new Source(properties), Executors.newFixedThreadPool(1));
            } else if (properties.getProperty("type").equals("sink")) {
                CompletableFuture.runAsync(new Sink(properties), Executors.newFixedThreadPool(1));
            } else {

                switch (properties.getProperty("function")) {
                    case "flatmap":
                        function = PipelineFunctions.FLATMAPPROCESSOR;
                        break;
                    case "filter":
                        function = PipelineFunctions.FILTERPROCESSOR;
                        break;
                    case "map":
                        function = PipelineFunctions.MAPPROCESSOR;
                        break;
                    case "windowaggregate":
                        function = PipelineFunctions.WINDOWAGGREGATEPROCESSOR;
                        break;
                }
                assert function != null;
                if (properties.getProperty("type").equals("stateless")) {
                    StageProcessor finalFunction = function;
                    CompletableFuture.runAsync(new StatelessAtomicProcessor(properties,
                            function), executor_stage).exceptionally(throwable -> {

                        //here we handle restart of crashed processors

                        DB dbc = DBMaker.fileDB("crashedThreads.db").fileMmapEnableIfSupported().make();
                        System.out.println("stateless processor restart");
                        ConcurrentMap<Integer, Pair<Integer, String>> mapc =
                                dbc.hashMap("crashedThreads", Serializer.INTEGER, Serializer.JAVA).createOrOpen();
                        System.out.println("size of current crashedThreadmap is: " + mapc.size());
                        for (Map.Entry<Integer, Pair<Integer, String>> crashed : mapc.entrySet()) {
                            System.out.println("restarting processor\t id : " + crashed.getKey() + "\t stagePos: "
                                    + crashed.getValue().getKey() + " : " + crashed.getValue().getValue());
                            if (crashed.getValue().getValue().equals("stateless")) {
                                properties.setProperty("simulateCrash", String.valueOf(0));
                                CompletableFuture.runAsync(new StatelessAtomicProcessor(properties, finalFunction),
                                        Executors.newFixedThreadPool(1));
                                mapc.remove(crashed.getKey(), crashed.getValue());

                            } else {
                                System.out.println("there is a queue of failed processes, scrolling");
                            }

                        }
                        dbc.close();
                        System.out.println("scrolled every entry of crashed threads");
                        return null;
                    });
                } else if (properties.getProperty("type").equals("stateful")) {
                    StageProcessor finalFunction = function;
                    CompletableFuture.runAsync(new StatefulAtomicProcessor(properties,
                            function), executor_stage).exceptionally(throwable -> {
                        //here we handle restart of crashed processors

                        DB dbc = DBMaker.fileDB("crashedThreads.db").fileMmapEnableIfSupported().make();
                        System.out.println("stateful processor restart");
                        ConcurrentMap<Integer, Pair<Integer, String>> mapc =
                                dbc.hashMap("crashedThreads", Serializer.INTEGER, Serializer.JAVA).createOrOpen();
                        System.out.println("size of current crashedThreadmap is: " + mapc.size());
                        for (Map.Entry<Integer, Pair<Integer, String>> crashed : mapc.entrySet()) {
                            System.out.println("restarting processor\t id : " + crashed.getKey() + "\t stagePos: "
                                    + crashed.getValue().getKey() + " : " + crashed.getValue().getValue());
                            if (crashed.getValue().getValue().equals("stateful")) {
                                properties.setProperty("simulateCrash", String.valueOf(0));
                                CompletableFuture.runAsync(new StatefulAtomicProcessor(properties, finalFunction),
                                        Executors.newFixedThreadPool(1));
                                mapc.remove(crashed.getKey(), crashed.getValue());

                            } else {
                                System.out.println("there is a queue of failed processes, scrolling");
                            }

                        }
                        dbc.close();
                        System.out.println("scrolled every entry of crashed threads");
                        return null;
                    });
                } else throw new RuntimeException("processor cannot start");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
