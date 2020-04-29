package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public interface Processor extends Runnable {

    void init();

    void process(ConsumerRecord<String, String> record);


}
