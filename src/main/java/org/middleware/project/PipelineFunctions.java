package org.middleware.project;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.middleware.project.Processors.*;

import java.util.*;

public class PipelineFunctions {

    private List<StageProcessor> processors;
    private String name;


    public PipelineFunctions(List<StageProcessor> processors, String name) {
        this.processors = processors;
        this.name = name;
    }



    public static final PipelineFunctions pipeline = new PipelineFunctions(Arrays.asList(

            new FilterProcessor((String k, String v) -> k.hashCode() % 2 == 1)

            /*,new MapProcessor((String key, String value)->{
                HashMap<String,String> result = new HashMap<>();
                result.put(value,"ABC");
                return result;
            })*/, new FlatMapProcessor((String key, String value) -> {
                HashMap<String, List<String>> result = new HashMap<>();
                result.put(key, Arrays.asList("111","222"));
                return result;
            })
            , new WindowedAggregateProcessor((String key, List<String> values) ->{//FIXME use mapdb
                String res = "";
                for(String v: values) {
                    res = res.concat(v);
                }
                HashMap <String, String> res_aggr = new HashMap<>();
                res_aggr.put(key,res);
                return res_aggr;
            },5,1,3),

            new FilterProcessor((String k, String v) -> k.hashCode() % 2 == 1){

            }), "pipeline1");

    public List<StageProcessor> getProcessors() {
        return processors;
    }

    public String getName() {
        return name;
    }
}
