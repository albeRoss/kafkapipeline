package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public abstract class StageProcessor {

    protected String stageGroup;


    /**
     * Processes received record based on the semantics of the function the processor embeds
     * @param record : the <key,value> pair pulled by this processor
     * @return <code>HashMap</code> : the result of the process function in the form of an hasmap
     */
    public abstract HashMap process(ConsumerRecord<String, String> record);
}
