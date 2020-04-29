package org.middleware.project;

import org.middleware.project.Processors.StageProcessor;
import org.middleware.project.Processors.WindowedAggregateProcessor;
import org.middleware.project.functions.WindowedAggregate;


import java.io.*;
import java.util.*;

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
        }*/
        StageProcessor stage = new WindowedAggregateProcessor(new HashMap<>(), (String key, List<String> values) ->{
            String res = "";
            for(String v: values) {
                res.concat(v);
            }
            HashMap <String, String> res_aggr = new HashMap<>();
            res_aggr.put(key,res);
            return res_aggr;
        },5,1);

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
                                + "List of Top Countries : " + value));



    }
}

