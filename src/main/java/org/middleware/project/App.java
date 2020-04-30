package org.middleware.project;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.Processors.WindowedAggregateProcessor;
import org.middleware.project.functions.WindowedAggregate;


import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

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
    public static void main(String[] args) throws IOException {
        // Collection based streams shouldn't be closed
        /*Stream.of("Red", "Blue", "Green", "Yellow", "Purple", "Back", "Fiorentina", "Juventus", "Lecce", "Emiliano")
                .map(String::toUpperCase).flatMap(x-> Arrays.stream(x.split(""))).distinct()
                //.filter(c -> c.length() > 4)
                .forEach(System.out::print);

        String[] colors = {"Red", "Blue", "Green"};*/
        // Arrays.stream(colors).map(String::toUpperCase).forEach(System.out::println);

        // IO-Based Streams Should be Closed via Try with Resources
        /*try (Stream<String> lines = Files.lines(Paths.get("/path/tp/file"))) {
            // lines will be closed after exiting the try block
        }
        StageProcessor stage = new WindowedAggregateProcessor(PipelineFunctions.hTreeMap, (String key, List<String> values) ->{
            String res = "";
            for(String v: values) {
                res.concat(v);
            }
            HashMap <String, String> res_aggr = new HashMap<>();
            res_aggr.put(key,res);
            return res_aggr;
        },5,1);*/
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

        DB db2 = app.openDBSession();

        ConcurrentMap<String, List<String>> windows = app.getWindows(db2);
        //windows.putIfAbsent("id", Arrays.asList("1", "2", "3"));
        System.out.println(windows.get("id"));

        ConcurrentMap<String, List<String>> map = (ConcurrentMap<String, List<String>>) windows;
        System.out.println(map.values());
        //db2.commit();
        db2.close();
        //System.out.println(windows);
        System.out.println(map.values());





    }
}

