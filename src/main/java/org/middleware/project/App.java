package org.middleware.project;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {

        System.out.println( "Hello World!" );
        CrunchifyGetPropertyValues properties = new CrunchifyGetPropertyValues();
        properties.getPropValues();
    }

    }

