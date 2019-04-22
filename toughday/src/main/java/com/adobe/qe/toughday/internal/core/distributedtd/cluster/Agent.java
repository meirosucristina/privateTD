package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.RedistributionRequestProcessor;
import com.google.gson.Gson;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.*;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Agent component for running TD distributed on Kubernetes.
 */
public class Agent {
    private static final String PORT = "4567";
    public static final String NAME_PREFIX = "Agent";

    // available routes
    private static final String SUBMIT_TASK_PATH = "/submitTask";
    private static final String FINISH_PATH = "/finish";
    private static final String HEARTBEAT_PATH = "/heartbeat";
    private static final String REBALANCE_PATH = "/rebalance";
    public static final String HEALTH_PATH = "/health";

    protected static final Logger LOG = LogManager.getLogger(Agent.class);

    public static String getFinishPath(String agentItAddress) {
        return URL_PREFIX + agentItAddress + ":" + PORT + FINISH_PATH;
    }

    public static String getHeartbeatPath(String agentIpAddress) {
        return URL_PREFIX + agentIpAddress + ":" + PORT + HEARTBEAT_PATH;
    }

    public static String getSubmissionTaskPath(String agentIpAdress) {
        return URL_PREFIX + agentIpAdress + ":" + PORT + SUBMIT_TASK_PATH;
    }

    public static String getRebalancePath(String agentIp) {
        return  URL_PREFIX + agentIp + PORT + REBALANCE_PATH;
    }

    private Engine engine;
    private String ipAddress = "";
    private final RedistributionRequestProcessor redistributionRequestProcessor = new RedistributionRequestProcessor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private volatile boolean finished = false;

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
            LOG.info("[Agent] Received execution request");

            String yamlTask = request.body();
            Configuration configuration = new Configuration(yamlTask);

            // TODO: change this to be executed in a different thread. The driver should know that the task was received by the agent!
            this.engine = new Engine(configuration);
            this.engine.runTests();
            LOG.info("[Agent] Successfully completed ToughDay task execution");

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

            return gson.toJson(currentCounts);
        }));

        post(REBALANCE_PATH, (((request, response) ->  {
            // this agent has recently joined the cluster => skip this request for now.
            if (this.engine == null) {
                return "";
            }

            String instructionsMessage = request.body();
            LOG.info("[Agent] Received " + instructionsMessage  + " from driver");
            this.redistributionRequestProcessor.processRequest(request, this.engine.getCurrentPhase());

            return "";
        })));


        get(HEALTH_PATH, ((request, response) -> "Healthy"));

        post(FINISH_PATH, ((request, response) -> {
            LOG.info("Finished work");
            this.finished = true;
            return "";
        }));

        // wait for requests
        while (!finished) {}
    }

    /**
     * Method responsible for registering the current agent to the driver. It should be the
     * first method executed.
     * It might take a while for the driver to send a response to the agent(in case redistribution is
     * executing) so the request should be asynchronous.
     */
    private void register() {
        /* send register request to the driver */
        HttpGet registerRequest = new HttpGet(Driver.getAgentRegisterPath() + "?ip=" + this.ipAddress);

        // TODO: change this! Driver must confirm that the agent was registered.
        /* submit request and check response code */
        asyncClient.execute(registerRequest, null);
    }
}
