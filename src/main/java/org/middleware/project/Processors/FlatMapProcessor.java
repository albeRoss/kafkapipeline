package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.middleware.project.functions.FlatMap;

import java.util.HashMap;

public class FlatMapProcessor extends StageProcessor{


    private FlatMap flatmap;

    public FlatMapProcessor(FlatMap flatmap) {
        this.flatmap = flatmap;
    }

    public HashMap process(final ConsumerRecord<String, String> record){

        String key = record.key();
        String value = record.value();

        return flatmap.flatmap(key,value);
    }


    public FlatMap getFlatmap() {
        return flatmap;
    }

    public void setFlatmap(FlatMap flatmap) {
        this.flatmap = flatmap;
    }
}
