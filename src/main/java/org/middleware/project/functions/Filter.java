package org.middleware.project.functions;

@FunctionalInterface
public interface Filter extends Function {
    public Boolean predicate(String key, String value);
}
