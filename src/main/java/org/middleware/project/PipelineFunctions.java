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

    public static FilterProcessor FILTERPROCESSOR = new FilterProcessor((String k, String v) -> k.hashCode() % 2 == 1);

    public static MapProcessor MAPPROCESSOR = new MapProcessor((String key, String value)->{
        HashMap<String,String> result = new HashMap<>();
        result.put(key,value.toUpperCase());
        return result;
    });

    public static FlatMapProcessor FLATMAPPROCESSOR = new FlatMapProcessor((String key, String value) -> {
        HashMap<String, List<String>> result = new HashMap<>();
        List<String> flatten = new ArrayList<>();
        for (int i = 0; i < value.length(); i++) {
            flatten.add(value.substring(i, i + 1));
        }
        result.put(key, flatten);
        return result;
    });

    public static WindowedAggregateProcessor WINDOWAGGREGATEPROCESSOR = new WindowedAggregateProcessor((String key, List<String> values) -> {
        String res = "";
        for (String v : values) {
            res = res.concat(v);
        }
        HashMap<String, String> res_aggr = new HashMap<>();
        res_aggr.put(key, res);
        return res_aggr;
    }, 3, 1, 1);



    public PipelineFunctions(List<StageProcessor> processors, String name) {
        this.processors = processors;
        this.name = name;
    }


    public static final PipelineFunctions pipeline_1 = new PipelineFunctions(Arrays.asList(

            //new FilterProcessor((String k, String v) -> k.hashCode() % 2 == 1)

           /* new MapProcessor((String key, String value)->{
                HashMap<String,String> result = new HashMap<>();
                result.put(key,value.toUpperCase());
                return result;
            }), new FlatMapProcessor((String key, String value) -> {
                HashMap<String, List<String>> result = new HashMap<>();
                result.put(key, Arrays.asList("A","msg"));
                return result;
            })*/
           new WindowedAggregateProcessor((String key, List<String> values) -> {
                String res = "";
                for (String v : values) {
                    res = res.concat(v);
                }
                HashMap<String, String> res_aggr = new HashMap<>();
                res_aggr.put(key, res);
                return res_aggr;
            }, 3, 1, 1)  // NB YOU MUST PUT THE RIGHT POSITION: pipelineLength <= stagePos >=1
            /*, new FilterProcessor((String k, String v) -> k.hashCode() % 2 == 1)*/
             , new FlatMapProcessor((String key, String value) -> {
                HashMap<String, List<String>> result = new HashMap<>();
                List<String> flatten = new ArrayList<>();
                for (int i = 0; i < value.length(); i++) {
                    flatten.add(value.substring(i, i + 1));
                }
                result.put(key, flatten);
                return result;
            })
    )
            , "pipeline1");

    public static final PipelineFunctions pipeline_2 = new PipelineFunctions(Arrays.asList(

            new FilterProcessor((String k, String v) -> k.hashCode() % 2 == 1)

            /*,new MapProcessor((String key, String value)->{
                HashMap<String,String> result = new HashMap<>();
                result.put(value,"ABC");
                return result;
            }), new FlatMapProcessor((String key, String value) -> {
                HashMap<String, List<String>> result = new HashMap<>();
                result.put(key, Arrays.asList("A","msg"));
                return result;
            })
           */, new WindowedAggregateProcessor((String key, List<String> values) -> {
                String res = "";
                for (String v : values) {
                    res = res.concat(v);
                }
                HashMap<String, String> res_aggr = new HashMap<>();
                res_aggr.put(key, res);
                return res_aggr;
            }, 3, 1, 2)  // NB YOU MUST PUT THE RIGHT POSITION: pipelineLength <= stagePos >=1
            //, new FilterProcessor((String k, String v) -> k.hashCode() % 2 == 1)
            , new FlatMapProcessor((String key, String value) -> {
                HashMap<String, List<String>> result = new HashMap<>();
                List<String> flatten = new ArrayList<>();
                for (int i = 0; i < value.length(); i++) {
                    flatten.add(value.substring(i, i + 1));
                }
                result.put(key, flatten);
                return result;
            })
    )
            , "pipeline1");

    public List<StageProcessor> getProcessors() {
        return processors;
    }

    public String getName() {
        return name;
    }
}
