package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;


import static spark.Spark.get;
import static spark.Spark.post;

public class Agent {
    private static final String DRIVER_HTTP = "http://driver";
    private static final String PORT = "80";
    private static final String DRIVER_REGISTER_PATH = "/registerAgent";
    private static final String HEARTBEAT_PATH = "/heartbeat";
    private static final String TASK_PATH = "/submitTask";
    protected static final Logger LOG = LogManager.getLogger(Agent.class);
    private String ipAddress = "";

    public void start() {

        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        register();

        post(TASK_PATH, ((request, response) ->  {
            String yamlTask = request.body();
            Configuration configuration = new Configuration(yamlTask);

            Engine engine = new Engine(configuration);
            engine.runTests();
            LOG.log(Level.INFO, "Successfully completed TD task execution");

            return "";
        }));

        get(HEARTBEAT_PATH, ((request, response) -> "Heartbeat acknowledged"));

        while (true) {}
    }

    /**
     * Method responsible for registering the current agent to the driver. Should be the
     * first method executed.
     */
    private void register() {
        /* send register request to K8s driver */
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpGet registerRequest = new HttpGet(DRIVER_HTTP + ":" + PORT + DRIVER_REGISTER_PATH + "/:" + this.ipAddress);

            /* submit request and check response code */
            HttpResponse driverResponse = httpClient.execute(registerRequest);
            LOG.log(Level.INFO, "Driver response code for registration request " +
                    driverResponse.getStatusLine().getStatusCode());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
