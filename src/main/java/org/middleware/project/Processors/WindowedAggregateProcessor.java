package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.middleware.project.functions.WindowedAggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WindowedAggregateProcessor extends StageProcessor{

    private final Map<String, List<String>> windows; // the internal state shared among processor of the same consumer group
    private WindowedAggregate windowedAggregate;
    private int windowSize;
    private int slide;

    public WindowedAggregateProcessor(Map<String, List<String>> windows, WindowedAggregate windowedAggregate, int windowSize, int slide) {
        this.windows = windows;
        this.windowedAggregate = windowedAggregate;
        this.windowSize = windowSize;
        this.slide = slide;
    }

    public HashMap process(final ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        // List of current values of the window
        List<String> winValues = windows.get(key);

        synchronized (windows){

            if (winValues == null) { // if the list is empty
                winValues = new ArrayList<>();
                winValues.add(value);

                windows.put(key, winValues);
            }else if (winValues.size() == windowSize) { // If the size is reached
                winValues.add(value);


                windows.put(key, winValues.subList(slide, winValues.size())); // Slide window
            }else{
                winValues.add(value);


                windows.put(key, winValues);


            }
        }

        windows.forEach(
                (k, v)->System.out.println(
                        "key : " + k + "\t\t"
                                + "values: " + v + "aggregate: "+
                                windowedAggregate.aggregate(k, windows.get(k))));
        return windowedAggregate.aggregate(key, windows.get(key));

    }

    public WindowedAggregate getWindowedAggregate() {
        return windowedAggregate;
    }

    public void setWindowedAggregate(WindowedAggregate windowedAggregate) {
        this.windowedAggregate = windowedAggregate;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int getSlide() {
        return slide;
    }

    public void setSlide(int slide) {
        this.slide = slide;
    }
}
