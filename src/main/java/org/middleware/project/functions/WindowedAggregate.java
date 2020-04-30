package org.middleware.project.functions;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.List;

@FunctionalInterface
public interface WindowedAggregate extends Function {
    public HashMap<String,String> aggregate(String key, List<String> values);
}
