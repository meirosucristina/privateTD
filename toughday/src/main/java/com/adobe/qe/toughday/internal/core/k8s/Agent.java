package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.AsyncTestWorker;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Agent component for running TD distributed in Kubernetes.
 */
public class Agent {
    private static final String PORT = "80";
    private static final String DRIVER_REGISTER_PATH = "/registerAgent";
    private static final String HEARTBEAT_PATH = "/heartbeat";
    private static final String TASK_PATH = "/submitTask";
    private static final String REBALANCE_PATH = "/rebalance";
    protected static final Logger LOG = LogManager.getLogger(Agent.class);

    private String ipAddress = "";
    private Engine engine;
    private AtomicBoolean rebalanceRequest = new AtomicBoolean(false);

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

            this.engine = new Engine(configuration);
            this.engine.runTests();
            LOG.log(Level.INFO, "Successfully completed TD task execution");

            return "";
        }));

        post(REBALANCE_PATH, (((request, response) ->  {
            LOG.log(Level.INFO, "Received request to interrupt TD execution.\n");
            System.out.println("Received request to interrupt TD execution.\n");
            this.rebalanceRequest.set(true);

            // we should interrupt all worker threads in order to stop running tests
            this.engine.getCurrentPhase().getRunMode().getRunContext().interruptWorkers();
            LOG.log(Level.INFO, "All test workers were successfully interrupted.\n");
            System.out.println("All test workers were successfully interrupted.\n");

            /* prevent run mode from creating/interrupting test worker threads during ramp up
             * or ramp down until work is completely rebalanced.
             */
            this.engine.getCurrentPhase().getRunMode().interruptRunMode(true);
            LOG.log(Level.INFO, "Run mode is no longer creating/deleting worker threads.\n");
            System.out.println("Run mode is no longer creating/deleting worker threads.\n");

            return AgentStates.WAITING_BALANCING_INSTRUCTIONS;
        })));

        get(HEARTBEAT_PATH, ((request, response) ->
        {
            // send to driver the total number of executions/test
            Gson gson = new Gson();
            Map<String, Long> currentCounts = new HashMap<>();

            // check if execution has started
            if (engine == null) {
                return gson.toJson(currentCounts);
            }

            Map<AbstractTest, AtomicLong> phaseCounts = engine.getCurrentPhase().getCounts();
            phaseCounts.forEach((test, count) -> currentCounts.put(test.getName(), count.get()));

            LOG.log(Level.INFO, "Successfully sent count properties to the driver\n.");

            return gson.toJson(currentCounts);
        }));

        get("/health", ((request, response) -> "Healthy"));

        // TODO: change this to automatically kill the pods
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
            HttpGet registerRequest = new HttpGet("http://driver" + ":" +
                    PORT + DRIVER_REGISTER_PATH + "/:" + this.ipAddress);

            /* submit request and check response code */
            HttpResponse driverResponse = httpClient.execute(registerRequest);
            LOG.log(Level.INFO, "Driver response code for registration request " +
                    driverResponse.getStatusLine().getStatusCode());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*class WorkerThreadsStateChecker implements Runnable {

    }*/
}
