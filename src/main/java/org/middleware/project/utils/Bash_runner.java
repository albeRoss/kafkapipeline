package org.middleware.project.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Bash_runner implements Runnable {

    @Override
    public void run() {
        File sh_path = new File(System.getProperty("user.dir")+"/../kafka_2.12-2.3.1/");
        try {
            Process chmod = Runtime.getRuntime().exec("chmod 755 start_kafka_cluster.sh", null, sh_path);
            chmod.waitFor();
            Process proc = Runtime.getRuntime().exec("/bin/bash -c ./start_kafka_cluster.sh", null, sh_path);
            proc.waitFor();

            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));


            String line = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            line = "";
            while ((line = errorReader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
