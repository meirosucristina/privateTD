package com.adobe.qe.toughday.internal.core.k8s;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

public class Agent {

    private static final String HOSTNAME = "HOSTNAME";
    private static final String DRIVER_HTTP = "http://driver";
    private static final String PORT = "4567";
    private static final String DRIVER_REGISTER_PATH = "/registerAgent";

    private final String hostname;

    public Agent() {
        this.hostname = System.getenv(HOSTNAME);
    }

    public void start() {
        register();

        /* make agent loop forever */
        while(true) {

        }
    }

    /**
     * Method responsible for registering the current agent to the driver. Should be the
     * first method executed.
     */
    public void register() {
        /* send register request to K8s driver */
        try {

            System.out.println("Registering....");

            URL url = new URL(DRIVER_HTTP + ":" + PORT + DRIVER_REGISTER_PATH + "/:" + this.hostname );
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();

            System.out.println("Response code is " + conn.getResponseCode());

            conn.disconnect();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
