package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.RedistributionRequestProcessor;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.*;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Agent component for running TD distributed on Kubernetes.
 */
public class Agent {
    private static final String PORT = "4567";
    private static final int HTTP_REQUEST_RETRIES = 3;
    private final ExecutorService tdExecutorService = Executors.newFixedThreadPool(1);

    // available routes
    private static final String SUBMIT_TASK_PATH = "/submitTask";
    private static final String FINISH_PATH = "/finish";
    private static final String HEARTBEAT_PATH = "/heartbeat";
    private static final String REBALANCE_PATH = "/rebalance";
    public static final String HEALTH_PATH = "/health";

    protected static final Logger LOG = LogManager.getLogger(Engine.class);
    private HttpUtils httpUtils = new HttpUtils();

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
        return  URL_PREFIX + agentIp + ":" +  PORT + REBALANCE_PATH;
    }

    private Engine engine;
    private String ipAddress = "";
    private final RedistributionRequestProcessor redistributionRequestProcessor = new RedistributionRequestProcessor();
    private volatile boolean finished = false;

    private boolean announcePhaseCompletion() {
        /* the current master might be dead so we should retry this for a certain amount of time before shutting
         * down the execution.
         */
        boolean successfullyAnnouncedDriver = false;
        long duration = GlobalArgs.parseDurationToSeconds("60s");
        while (duration > 0) {
            successfullyAnnouncedDriver =
                    this.httpUtils.sendSyncHttpRequest(this.ipAddress, Driver.getPhaseFinishedByAgentPath(), HTTP_REQUEST_RETRIES);
            try {
                Thread.sleep(10 * 1000L); // try again in 10 seconds
            } catch (InterruptedException e) {
                // skip this since this thread is generally not interrupted by anyone
            } finally {
                duration -= GlobalArgs.parseDurationToSeconds("10s");
            }
        }

        return successfullyAnnouncedDriver;
    }

    public void start() {
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        register();

        /* Expose http endpoint for receiving ToughDay execution request from the driver */
        post(SUBMIT_TASK_PATH, ((request, response) ->  {
            LOG.info("[Agent] Received execution request");

            String yamlTask = request.body();
            Configuration configuration = new Configuration(yamlTask);

            tdExecutorService.submit(() ->  {
                this.engine = new Engine(configuration);
                this.engine.runTests();

                if (!announcePhaseCompletion()) {
                    /* we reach a situation in which drivers are no longer able to respond to http requests
                     * requests sent by the agents => finish execution.
                     */
                    LOG.error("Agent " + this.ipAddress + " could not inform driver that phase was executed.");
                    System.exit(-1);
                }
            });

            return "";
        }));

        /* Expose http endpoint to be used by the driver for heartbeat messages */
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

        /* expose http endpoint for receiving redistribution requests from the driver */
        post(REBALANCE_PATH, (((request, response) ->  {
            // this agent has recently joined the cluster => skip this request for now.
            if (this.engine == null || this.engine.getCurrentPhase() == null) {
                LOG.info("Agent " + this.ipAddress + " is skipping rebalancing for now...");
                return "";
            }

            String instructionsMessage = request.body();
            LOG.info("[Agent] Received " + instructionsMessage  + " from driver");
            this.redistributionRequestProcessor.processRequest(request, this.engine.getCurrentPhase());

            return "";
        })));


        /* expose http endpoint for health checks */
        get(HEALTH_PATH, ((request, response) -> "Healthy"));

        /* expose http endpoint for finishing the execution of the agent */
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
        boolean successfullyRegistered =
                this.httpUtils.sendSyncHttpRequest(this.ipAddress, Driver.getAgentRegisterPath(), HTTP_REQUEST_RETRIES);
        if (!successfullyRegistered) {
            System.exit(-1);
        }

    }
}
