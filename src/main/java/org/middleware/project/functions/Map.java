package org.middleware.project.functions;

import java.util.HashMap;

@FunctionalInterface
public interface Map extends Function  {

    public HashMap<String,String> map(String key, String value);

}
