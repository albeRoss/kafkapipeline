package org.middleware.project;

import org.middleware.project.Processors.*;
import java.util.*;

/**
 * ensamble of functions
 */
public class PipelineFunctions {


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
    }, 3, 1, 1); // these are default values, they are set at runtime


}
