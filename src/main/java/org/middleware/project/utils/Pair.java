package org.middleware.project.utils;

import java.io.Serializable;

public class Pair<T,U> implements Serializable {
    private final T key;
    private final U value;

    public Pair(T key, U value) {
        this.key = key;
        this.value = value;
    }

    public T getKey() {
        return this.key;
    }

    public U getValue() {
        return this.value;
    }
}