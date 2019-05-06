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
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;


import static com.adobe.qe.toughday.internal.core.engine.Engine.installToughdayContentPackage;
import static spark.Spark.*;

/**
 * Driver component for K8s cluster.
 */
public class Driver {

    private static final String URL_PREFIX = "http://";
    private static final String PORT = "4567";
    private static final String REGISTRATION_PARAM = ":ipAddress";
    private static final String REGISTER_PATH = "/registerAgent/" + REGISTRATION_PARAM;
    private static final String EXECUTION_PATH = "/config";
    private static final String HEARTBEAT_PATH = "/heartbeat";
    private static final String SUBMIT_TASK_PATH = "/submitTask";
    private static final String AGENT_PREFIX_NAME = "Agent";

    protected static final Logger LOG = LogManager.getLogger(Agent.class);
    private static final AtomicInteger id = new AtomicInteger(0);
    private long heartbeatInterval = GlobalArgs.parseDurationToSeconds("5s");

    private final ConcurrentHashMap<String, String> agents = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private final Map<String, Future<HttpResponse>> runningTasks = new HashMap<>();

    private ExecutorService executorService = Executors.newFixedThreadPool(1);

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

        PhasePartitioner phasePartitioner = new PhasePartitioner();
        configuration.getPhases().forEach(phase -> {
            try {
                Map<String, Phase> tasks = phasePartitioner.splitPhase(phase, new ArrayList<>(agents.keySet()));

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

                        Future<HttpResponse> taskResponse = asyncClient.execute(taskRequest, null);
                        runningTasks.put(agentId, taskResponse);

                        LOG.log(Level.INFO, "Task was submitted to agent " + agents.get(agentId));

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                // we should wait until all agents complete the current tasks in order to execute phases sequentially
                Future future = executorService.submit(new StatusCheckerWorker());
                future.get();

                LOG.log(Level.INFO, "Phase " + phase.getName() + " finished execution successfully.");

            } catch (CloneNotSupportedException | ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    private int executeHeartbeatRequest(HttpClient heartBeatHttpClient, HttpGet heartbeatRequest) {
        try {
            HttpResponse agentResponse = heartBeatHttpClient.execute(heartbeatRequest);
            return agentResponse.getStatusLine().getStatusCode();
        } catch (IOException e) {
            return -1;
        }
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

            return "";
        }));

        get("/health", ((request, response) -> "Healthy"));

        /* expose http endpoint for registering new agents to the cluster */
        get(REGISTER_PATH, (request, response) -> {
            String agentIp = request.params(REGISTRATION_PARAM).replaceFirst(":", "");
            agents.put(AGENT_PREFIX_NAME + id.getAndIncrement(), agentIp);

            LOG.log(Level.INFO, "Registered agent with ip " + agentIp);
            return "";
        });

        // we should periodically send heartbeat messages from driver to all the agents
        heartbeatScheduler.scheduleAtFixedRate(() ->
        {
            for (String agentId : agents.keySet()) {
                CloseableHttpClient heartBeatHttpClient = HttpClientBuilder.create().build();

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

                int retrial = 3;
                int responseCode = 0;
                while (responseCode != 200 && retrial > 0) {
                    responseCode = executeHeartbeatRequest(heartBeatHttpClient, heartbeatRequest);
                    retrial -= 1;
                }

                if (retrial <= 0) {
                    LOG.log(Level.INFO, "Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                    agents.remove(agentId);

                    // TODO: change this when the new pod is able to resume the task
                    runningTasks.remove(agentId);
                }

                try {
                    heartBeatHttpClient.close();
                } catch (IOException e) {
                    LOG.warn("HeartBeat apache http client could not be closed.");
                }
            }
        }, 0, heartbeatInterval, TimeUnit.SECONDS);

        /* wait for requests */
        while (true) { }
    }

    private class StatusCheckerWorker implements Runnable {
        @Override
        public synchronized void run() {
            long size = runningTasks.keySet().size();

            while (size > 0) {
                size = runningTasks.entrySet().stream()
                        .filter(entry -> !entry.getValue().isDone()).count();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // skip and continue
                }
            }

            // reset map of running tasks
            runningTasks.clear();
        }
    }
}
