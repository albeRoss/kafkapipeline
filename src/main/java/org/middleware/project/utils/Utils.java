package org.middleware.project.utils;

import java.io.*;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Properties;

public class Utils {

    public static int[] distribute(int servers, int processors) {
        int[] distribution = new int[servers];
        for (int i = 0; i < processors; i++) {
            distribution[i % servers] += 1;

        }
        return distribution;
    }

    public static Properties loadEnvProperties(String fileName) {
        Properties prop = new Properties();
        try {

            InputStream inputStream = new FileInputStream("./" + fileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + fileName + "' not found in the classpath");
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }


    public static File[] finder(String dirName) {
        File dir = new File(dirName);
        if (!dir.exists())
            System.out.println(dir + " Directory doesn't exists");
        return dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String filename) {
                return filename.toLowerCase().endsWith(".properties");
            }
        });

    }


    public static void main(String[] args) {

        int day = 6;
        switch (day) {
            case 6:
                System.out.println("Today is Saturday");
                break;
            case 7:
                System.out.println("Today is Sunday");
                break;
            default:
                System.out.println("Looking forward to the Weekend");
        }


    }

};

