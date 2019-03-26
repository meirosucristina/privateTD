package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;


import static com.adobe.qe.toughday.internal.core.engine.Engine.installToughdayContentPackage;
import static spark.Spark.*;

/**
 * Driver component for K8s cluster.
 */
public class Driver {

    private static final String URL_PREFIX = "http://";
    private static final String SUBMIT_TASK_PATH = "/submitTask";
    private static final String PORT = "4567";
    private static final String REGISTRATION_PARAM = ":ipAddress";
    private static final String REGISTER_PATH = "/registerAgent/" + REGISTRATION_PARAM;
    private static final String EXECUTION_PATH = "/submitConfig";
    private static final String HEARTBEAT_PATH = "/heartbeat";
    private static final AtomicInteger id = new AtomicInteger(0);
    private static final String AGENT_PREFIX_NAME = "Agent";
    protected static final Logger LOG = LogManager.getLogger(Agent.class);

    private final ConcurrentHashMap<String, String> agents = new ConcurrentHashMap<>();
    private long heartbeatInterval = GlobalArgs.parseDurationToSeconds("5s");
    private ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private HttpClient httpClient = HttpClientBuilder.create().build();

    public Driver() {
        asyncClient.start();
    }


    private void handleToughdaySampleContent(Configuration configuration) {
        GlobalArgs globalArgs = configuration.getGlobalArgs();

        if (globalArgs.getInstallSampleContent() && !globalArgs.getDryRun()) {
            try {
                installToughdayContentPackage(globalArgs);
                globalArgs.setInstallSampleContent("false");

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleExecutionRequest(Configuration configuration) {
        handleToughdaySampleContent(configuration);

        TaskPartitioner taskPartitioner = new TaskPartitioner();
        configuration.getPhases().forEach(phase -> {
            try {
                Map<String, Phase> tasks = taskPartitioner.splitPhase(phase, new ArrayList<>(agents.keySet()));

                agents.keySet().forEach(agentId -> {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentId)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    try {
                        /* build HTTP query with yaml configuration as body */
                        String URI = URL_PREFIX + agents.get(agentId) + ":" + PORT + SUBMIT_TASK_PATH;

                        HttpPost taskRequest = new HttpPost(URI);
                        StringEntity params = new StringEntity(yamlTask);
                        taskRequest.setEntity(params);
                        taskRequest.setHeader("Content-type", "text/plain");

                        /* submit request and check response code */
                        /*HttpResponse agentResponse = httpClient.execute(taskRequest);

                        LOG.log(Level.INFO, "Agent response code: " + agentResponse.getStatusLine().getStatusCode()); */
                        asyncClient.execute(taskRequest, null);
                        LOG.log(Level.INFO, "Task was submitted to agent " + agents.get(agentId));

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        });
    }

    public void run() {
        /* expose http endpoint for running TD with the given configuration */
        post(EXECUTION_PATH, ((request, response) -> {
            String yamlConfiguration = request.body();
            Configuration configuration = new Configuration(yamlConfiguration);

            new Thread() {
                public synchronized void run() {
                    handleExecutionRequest(configuration);
                }
            }.start();

            return "received execution";
        }));

        /* expose http endpoint for registering new agents to the cluster */
        get(REGISTER_PATH, (request, response) -> {
            String agentIp = request.params(REGISTRATION_PARAM).replaceFirst(":", "");
            agents.put(AGENT_PREFIX_NAME + id.getAndIncrement(), agentIp);

            LOG.log(Level.INFO, "Registered agent with ip " + agentIp);
            return "";
        });


        // we should periodically send heartbeat messages from driver to all the agents
        Thread heartBeatWorkerThread = new Thread(new HeartBeatWorker());
        heartBeatWorkerThread.start();

        /* wait for execution command */
        while (true) { }
    }

    private class HeartBeatWorker implements Runnable {

        private int executeHeartbeatRequest(HttpClient heartBeatHttpClient, HttpGet heartbeatRequest) {
            try {
                HttpResponse agentResponse = heartBeatHttpClient.execute(heartbeatRequest);
                return agentResponse.getStatusLine().getStatusCode();
            } catch (IOException e) {
                return -1;
            }
        }

        @Override
        public synchronized void run() {
        try {
                heartbeatScheduler.scheduleAtFixedRate(() ->
                {
                    for (String agentId : agents.keySet()) {
                        HttpClient heartBeatHttpClient = HttpClientBuilder.create().build();

                        String ipAddress = agents.get(agentId);
                        String URI = URL_PREFIX + ipAddress + ":" + PORT + HEARTBEAT_PATH;
                        // configure timeout limits
                        RequestConfig requestConfig = RequestConfig.custom()
                                .setConnectionRequestTimeout(1000)
                                .setConnectTimeout(1000)
                                .setSocketTimeout(1000)
                                .build();
                        HttpGet heartbeatRequest = new HttpGet(URI);
                        heartbeatRequest.setConfig(requestConfig);

                        int retrial = 2;
                        int responseCode = executeHeartbeatRequest(heartBeatHttpClient, heartbeatRequest);
                        while (responseCode != 200 && retrial > 0) {
                            retrial -= 1;
                        }

                        LOG.log(Level.INFO, "Agent with ip " + ipAddress + " answered to heartbeat with " + responseCode);

                        if (retrial <= 0) {
                            LOG.log(Level.INFO, "Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                            agents.remove(agentId);
                        }
                    }
                }, 0, heartbeatInterval, TimeUnit.SECONDS);

            } catch (Exception e) {
                LOG.log(Level.INFO, "Exception thrown in heartbeat worker");
                LOG.log(Level.INFO, e.getMessage());
            }
        }
    }
}
