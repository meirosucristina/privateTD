package com.adobe.qe.toughday.internal.core.k8s.cluster;

import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceRequestProcessor;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;


import static com.adobe.qe.toughday.internal.core.k8s.HttpUtils.*;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Agent component for running TD distributed in Kubernetes.
 */
public class Agent {
    private static final String PORT = "80";
    private static final String DRIVER_REGISTER_PATH = "/registerAgent";
    protected static final Logger LOG = LogManager.getLogger(Agent.class);

    private Engine engine;
    private String ipAddress = "";
    private final RebalanceRequestProcessor rebalanceReqProcessor = new RebalanceRequestProcessor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();

    public void start() {
        asyncClient.start();

        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        register();

        post(SUBMIT_TASK_PATH, ((request, response) ->  {
            System.out.println("[Agent - Task path] Received execution request");
            String yamlTask = request.body();
            Configuration configuration = new Configuration(yamlTask);

            System.out.println("[Agent - Task path] Starting execution...");
            this.engine = new Engine(configuration);
            this.engine.runTests();
            LOG.log(Level.INFO, "Successfully completed TD task execution");

            return "";
        }));

        get(HEARTBEAT_PATH, ((request, response) ->
        {
            // send to driver the total number of executions/test
            Gson gson = new Gson();
            Map<String, Long> currentCounts = new HashMap<>();

            // check if execution has started
            if (engine == null || engine.getCurrentPhase() == null) {
                return gson.toJson(currentCounts);
            }

            Map<AbstractTest, AtomicLong> phaseCounts = engine.getCurrentPhase().getCounts();
            phaseCounts.forEach((test, count) -> currentCounts.put(test.getName(), count.get()));

            //LOG.log(Level.INFO, "Successfully sent count properties to the driver\n.");

            return gson.toJson(currentCounts);
        }));

        post(REBALANCE_PATH, (((request, response) ->  {
            if (this.engine == null) {
                // this agent has recently joined the cluster => skip this request
                return "ack";
            }

            String instructionsMessage = request.body();
            System.out.println("[rebalancing - agent] Received " + instructionsMessage + " from driver");
            this.rebalanceReqProcessor.processRequest(request, this.engine.getCurrentPhase());

            return "ack";
        })));


        get("/health", ((request, response) -> "Healthy"));

        // TODO: change this to automatically kill the pods
        while (true) {}
    }

    /**
     * Method responsible for registering the current agent to the driver. Should be the
     * first method executed.
     * It might take a while for the driver to send a response to the agent(in case rebalancing is
     * required) so the request should be asynchronous.
     */
    private void register() {
        /* send register request to K8s driver */
        HttpGet registerRequest = new HttpGet("http://driver" + ":" +
                PORT + DRIVER_REGISTER_PATH + "/:" + this.ipAddress);

        /* submit request and check response code */
        Future<HttpResponse> driverResponse = asyncClient.execute(registerRequest, null);
    }
}
