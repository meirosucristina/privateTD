package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.net.*;

import static spark.Spark.post;

public class Agent {

    private static final String DRIVER_HTTP = "http://driver";
    private static final String PORT = "4567";
    private static final String DRIVER_REGISTER_PATH = "/registerAgent";

    private String ipAddress = "";

    public void start() {

        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        register();

        post("/submitTask", ((request, response) ->  {
            String yamlTask = request.body();
            Configuration configuration = new Configuration(yamlTask);

            // start TD execution
            Thread thread = new Thread() {
                public synchronized void run () {
                    Engine engine = new Engine(configuration);
                    engine.runTests();
                }
            };

            thread.start();

            return "";
        }));

        /* wait until driver sends tasks */
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
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpGet registerRequest = new HttpGet(DRIVER_HTTP + ":" + PORT + DRIVER_REGISTER_PATH + "/:" + this.ipAddress);

            /* submit request and check response code */
            HttpResponse driverResponse = httpClient.execute(registerRequest);
            System.out.println("Response code is " + driverResponse.getStatusLine().getStatusCode());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
