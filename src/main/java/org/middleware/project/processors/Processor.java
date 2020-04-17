package org.middleware.project.processors;

public interface Processor extends Runnable {

    void init();

    void process();

    void punctuate();

}
