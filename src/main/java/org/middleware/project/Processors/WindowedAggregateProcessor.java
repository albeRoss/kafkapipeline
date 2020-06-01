package org.middleware.project.Processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.middleware.project.functions.WindowedAggregate;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class WindowedAggregateProcessor extends StageProcessor{


    private ConcurrentMap<String, List<String>> windows;
    private ConcurrentMap<String, List<String>> oldSlidedValues;
    //private HTreeMap<String, List<String>> windows = new HTreeMap<String, List<String>>(); // the internal state shared among processor of the same consumer group
    private WindowedAggregate windowedAggregate;
    private int windowSize;
    private int slide;
    private int pos;

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public WindowedAggregateProcessor(WindowedAggregate windowedAggregate, int windowSize, int slide, int stagePos) {
        this.windowedAggregate = windowedAggregate;
        this.windowSize = windowSize;
        this.slide = slide;
        this.stageGroup = "group_"+stagePos;
        this.pos = stagePos;
    }

    public WindowedAggregateProcessor clone(){
        return new WindowedAggregateProcessor(this.windowedAggregate,this.windowSize,this.slide,this.pos);
    }

    public HashMap process(final ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        // List of current values of the window
            List<String> winValues = windows.get(key);
            if (winValues == null) { // if the list is empty
                winValues = new ArrayList<>();
                winValues.add(value);
                windows.put(key, winValues);
            } else if (winValues.size() == windowSize) { // If the size is reached
                winValues.add(value);

                List<String> oldValues = new ArrayList<>(winValues.subList(0, winValues.size() - 1 - windowSize + slide));
                oldSlidedValues.clear();
                oldSlidedValues.put(key,oldValues); //save old values for possible revert

                List<String> slidedWindow = new ArrayList<>(winValues.subList(winValues.size() - 1 - windowSize + slide, winValues.size()));
                windows.put(key, slidedWindow); // Slide window

                //System.out.println("window slided");

            } else {
                winValues.add(value);

                windows.put(key, winValues);
            }
            System.out.println("[WINSTAGE] LOCAL STATE:");
            windows.forEach(
                    (k, v)->System.out.println(
                            "\tkey : " + k + "\t\t"
                                    + "values: " + v + "\t aggregate: "+
                                    windowedAggregate.aggregate(k, windows.get(k))));
            return windowedAggregate.aggregate(key, windows.get(key));

    }

    public void rollback(){
        if (oldSlidedValues.entrySet().size() !=1){
            throw new IndexOutOfBoundsException("bug: oldSlidedValues must have a single entry ");
        }
        oldSlidedValues.forEach( (k , v) -> {
            List<String> winValues = windows.get(k);
            List<String> slidedWindow = new ArrayList<>(winValues.subList(0,
                    winValues.size() - slide));
            slidedWindow.addAll(0,v);
            windows.put(k,slidedWindow);
        });
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

    public void setWindows(ConcurrentMap<String, List<String>> windows) {
        this.windows = windows;
    }

    public ConcurrentMap<String, List<String>> getWindows() {
        return windows;
    }

    public ConcurrentMap<String, List<String>> getOldSlidedValues() {
        return oldSlidedValues;
    }

    public void setOldSlidedValues(ConcurrentMap<String, List<String>> oldSlidedValues) {
        this.oldSlidedValues = oldSlidedValues;
    }
}
