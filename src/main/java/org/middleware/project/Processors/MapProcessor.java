package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.middleware.project.functions.Map;

import java.util.HashMap;

public class MapProcessor extends StageProcessor{

    private Map map;


    public MapProcessor(Map map) {
        this.map = map;
    }

    public HashMap process(ConsumerRecord<String, String> record){
        String key = record.key();
        String value = record.value();

        return map.map(key,value);
    }

    public Map getMap() {
        return map;
    }

    public void setMap(Map map) {
        this.map = map;
    }
}
