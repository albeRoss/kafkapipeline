package org.middleware.project;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.Processors.WindowedAggregateProcessor;
import org.middleware.project.functions.WindowedAggregate;
import org.middleware.project.topology.AtomicStage;


import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Hello world!
 *
 */
public class App


{


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
    public void prova(HTreeMap<String,List<String>> map){

        map.put("A",Arrays.asList("1","2"));


    }

    public void executeScript() throws IOException, InterruptedException {
        File sh_path = new File(System.getProperty("user.dir")+"/../kafka_2.12-2.3.1/");
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
    protected DB openDBSession(){
        return DBMaker
                .fileDB("prova.db")
                .transactionEnable()
                .make();
    }

    protected Long getOffset(DB db){
        HTreeMap<Integer, Long> processedOffsetsMap  = db.hashMap("processedOffsetsMap", Serializer.INTEGER, Serializer.LONG).createOrOpen();
        return processedOffsetsMap.get(0);
    }

    protected HTreeMap<String, List<String>> getWindows(DB db){
        return db.hashMap("windows", Serializer.STRING, Serializer.JAVA).createOrOpen();
    }


    public static void main(String[] args) throws IOException, InterruptedException {


/*
        Map<String, List<String>> continentTopContries =
                new HashMap<String, List<String>>();

        List<String> topCountriesOfAsiaContinent =
                new ArrayList<String>();

        // add top countries of Asian continent
        topCountriesOfAsiaContinent.add("India");
        topCountriesOfAsiaContinent.add("China");
        topCountriesOfAsiaContinent.add("Russia");
        topCountriesOfAsiaContinent.add("Japan");

        // put 1st entry as Asia and its list of Top countries
        continentTopContries.put("Asia",
                topCountriesOfAsiaContinent);

       // ConsumerRecord record1 = new ConsumerRecord<String,String>("A","B");
        //stage.process();
        WindowedAggregate function = (String key, List<String> values) ->{
            String res = "";
            for(String v: values) {
                res = res.concat(v);
            }
            System.out.println(res);
            HashMap <String, String> res_aggr = new HashMap<>();
            res_aggr.put(key,res);
            return res_aggr;
        };

        System.out.println(function.aggregate("A", Arrays.asList("A", "C", "D")).values().toString());


        if(true){
            System.out.println();
        }
        continentTopContries.forEach(
                (key, value)->System.out.println(
                        "Continent name : " + key + "\t\t"
                                + "List of Top Countries : " + value));*/


        App app = new App();
        /*DB db = DBMaker
                .fileDB("file2.db")
                .transactionEnable()
                .make();

        HTreeMap<String,List<String>> map = db
                .hashMap("map", Serializer.STRING, Serializer.JAVA)
                .createOrOpen();
        app.prova(map);
        */
        //map.put("something", Arrays.asList("1","2"));
        //db.commit();
        /*map.put("something2", "333L");

        System.out.println(map.get("something2"));
        db.rollback();
        System.out.println(map.get("something2"));
        System.out.println(map.getEntries());
        db.commit();
        db.close();
        db.

        DB db1 = DBMaker
                .fileDB("file2.db")
                .transactionEnable()
                .make();
        HTreeMap<String,List<String>> map1 = db1
                .hashMap("map", Serializer.STRING, Serializer.JAVA)
                .createOrOpen();
        db1.rollback();
        System.out.println(map1.getEntries());
        db1.close();*/


        // first case


        /*final int state = 4;
        Runnable runnableTask = () -> {
            try {
                while(true){
                    System.out.println("alive");
                    Thread.sleep(1000);
                }

            } catch (InterruptedException e) {
                System.out.println("crash: restart");
                e.printStackTrace();
                ExecutorService executor = Executors.newFixedThreadPool(1);
                Future obj = executor.submit(new Runnable() {
                    @Override
                    public void run() {
                            try {
                        while(true){
                            System.out.println("alive again");
                                Thread.sleep(1000);
                            System.out.println("state was: "+ state);
                        }
                            } catch (InterruptedException ex) {
                                ex.printStackTrace();
                            }
                    }
                });
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future obj = executor.submit(runnableTask);
        Thread.sleep(5000);

        obj.cancel(true);*/

        //second case

        /*int id = 1;
        String group = "group1";
        final int state = 4;
        Runnable runnableTask = () -> {
            int countDown = 4;

                while(true){
                    System.out.println("alive");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(countDown > 0) countDown--;
                    else {
                        DB db = DBMaker.fileDB("file.db").make();
                        ConcurrentMap map = db.hashMap("map").createOrOpen();
                        //we need the id of the processor and the position of the
                        map.put(1, 2);
                        db.close();
                        throw new RuntimeException("crash!");
                    }
                }
        };*/
        ExecutorService executor = Executors.newFixedThreadPool(5);
        //Future obj = executor.submit(runnableTask);

        Runnable runnable = () -> {
            int i = 5;
            while(i>0){
                System.out.println("runnable running");
                i--;
            }
        };

        CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                int i = 5;
                while(i>0){
                    System.out.println("hello");
                    i--;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }throw new RuntimeException("error");
            }
        },executor).exceptionally(throwable -> {
            System.out.println("error occurred");
            return null;
        }).thenRunAsync(new Runnable() {
            @Override
            public void run() {
                CompletableFuture.runAsync(runnable);
            }
        });



       /* try {
            obj.get();

        } catch (ExecutionException | RuntimeException e) {
            System.out.println("restart here");
            e.printStackTrace();
        }

        executor.shutdown();
        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        }

        System.out.println("here we end");*/


       /* DB db2 = app.openDBSession();

        ConcurrentMap<String, List<String>> windows = app.getWindows(db2);
        //windows.putIfAbsent("id", Arrays.asList("1", "2", "3"));
        System.out.println(windows.get("id"));

        ConcurrentMap<String, List<String>> map = (ConcurrentMap<String, List<String>>) windows;
        System.out.println(map.values());
        //db2.commit();
        db2.close();
        //System.out.println(windows);
        System.out.println(map.values());*/





    }
}

