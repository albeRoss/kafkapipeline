package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public abstract class StageProcessor {

    public abstract HashMap process(ConsumerRecord<String, String> record);
}
