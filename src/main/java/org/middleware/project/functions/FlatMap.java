package org.middleware.project.functions;

import java.util.HashMap;
import java.util.List;

@FunctionalInterface
public interface FlatMap extends Function {
    public HashMap<String,List<String>> flatmap(String key, String value);
}
