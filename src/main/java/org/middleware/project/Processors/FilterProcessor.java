package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.middleware.project.functions.Filter;

import java.util.HashMap;

public class FilterProcessor extends StageProcessor{

    private Filter filter;

    public FilterProcessor(Filter filter) {

        this.filter = filter;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public HashMap process(final ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();
        HashMap <String, String> res_filter = new HashMap<>();

        if (filter.predicate(key,value)) res_filter.put(key,value);

        return res_filter;
         // doesn't return the message back just commit to kafka and continue
    }
}
