package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
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
    private static final String REBALANCE_PATH = "/rebalance";
    private static final String AGENT_PREFIX_NAME = "Agent";

    protected static final Logger LOG = LogManager.getLogger(Agent.class);
    private static final AtomicInteger id = new AtomicInteger(0);
    private long heartbeatInterval = GlobalArgs.parseDurationToSeconds("1s");

    private final ConcurrentHashMap<String, String> agents = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private final Map<String, Future<HttpResponse>> runningTasks = new HashMap<>();

    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private Map<String, Map<String, Long>> counts = new HashMap<>();


    public Driver() {
        asyncClient.start();
    }

    private boolean wasExecutionTrigerred() {
        return !runningTasks.keySet().isEmpty();
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

    private void announceRebalancing() {
        List<Future<HttpResponse>> futures = new ArrayList<>();
        LOG.log(Level.INFO, "[Rebalancing] Driver starts announcing agents to interrupt their work.\n");
        System.out.println("[Rebalancing] Driver starts announcing agents to interrupt their work.\n");

        CloseableHttpAsyncClient rebalanceClient = HttpAsyncClients.createDefault();
        rebalanceClient.start();

        this.agents.forEach((agentName, agentIp) -> {
            System.out.println("[Rebalancing] Sending request to agent " + agentIp);
            String URI = URL_PREFIX + agentIp + ":" + PORT + REBALANCE_PATH;
            HttpPost rebalanceRequest = new HttpPost(URI);

            Future<HttpResponse> agentResponse = rebalanceClient.execute(rebalanceRequest, null);
            System.out.println("[Rebalancing] Request sent to " + agentIp);
            futures.add(agentResponse);
        });

        // wait until all agents have successfully reached the interrupted state.
        futures.forEach(future -> {
            try {
                System.out.println("[Realancing] Waiting for future to finish");
                HttpResponse httpResponse = future.get();
                LOG.log(Level.INFO, "[Rebalancing] Response code is " + httpResponse.getStatusLine().getStatusCode() + "\n");
                System.out.println("[Rebalancing] Response code is " + httpResponse.getStatusLine().getStatusCode() + "\n");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        try {
            rebalanceClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleExecutionRequest(Configuration configuration) {
        handleToughdaySampleContent(configuration);

        TaskPartitioner taskPartitioner = new TaskPartitioner();
        configuration.getPhases().forEach(phase -> {
            try {
                // set number of executions/test to zero
                counts = new HashMap<>();
                phase.getTestSuite().getTests().forEach(test -> counts.put(test.getName(), new HashMap<>()));

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
                System.out.println("Phase " + phase.getName() + " finished execution successfully.");

            } catch (CloneNotSupportedException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private Map<String, Long> getExecutionsPerTest() {
        Map<String, Long> executionsPerTest = new HashMap<>();

        this.counts.keySet().forEach(testName -> executionsPerTest.put(testName, 0L));

        this.counts.forEach((testName, executionsPerAgent) -> {
            executionsPerTest.put(testName, executionsPerAgent.values().stream().mapToLong(x -> x).sum());
        });

        return executionsPerTest;
    }

    private int executeHeartbeatRequest(HttpClient heartBeatHttpClient, HttpGet heartbeatRequest, String agentName) {
        try {
            HttpResponse agentResponse = heartBeatHttpClient.execute(heartbeatRequest);
            int responseCode = agentResponse.getStatusLine().getStatusCode();

            if (responseCode == 200) {
                // the agent has sent his statistic for executions/test => aggregate counts
                Gson gson = new Gson();
                String yamlCounts =  EntityUtils.toString(agentResponse.getEntity());

                try {
                    // gson treats numbers as double values by default
                    Map<String, Double> doubleAgentCounts = gson.fromJson(yamlCounts, Map.class);
                    //System.out.println("Am primit count updates de la agentul " + agentName + ": " + doubleAgentCounts.toString());
                    this.counts.forEach((testName, executionsPerAgent) ->
                            this.counts.get(testName).put(agentName, doubleAgentCounts.get(testName).longValue()));

                } catch (Exception e) {
                    System.out.println("Executions/test were not successfully received from " + this.agents.get(agentName));
                    LOG.warn("Executions/test were not successfully received from " + this.agents.get(agentName));
                }
            } else {
                System.out.println("Response code != 200 for agent " + agentName + ". Value = " + responseCode);
            }

            return responseCode;
        } catch (IOException e) {
            return -1;
        }
    }

    public void run() {
        /* expose http endpoint for running TD with the given configuration */
        post(EXECUTION_PATH, ((request, response) -> {
            String yamlConfiguration = request.body();
            Configuration configuration = new Configuration(yamlConfiguration);

            new Thread(() -> handleExecutionRequest(configuration)).start();

            return "";
        }));

        get("/health", ((request, response) -> "Healthy"));

        /* expose http endpoint for registering new agents to the cluster */
        get(REGISTER_PATH, (request, response) -> {
            System.out.println("WAS EXECUTION TRIGGERED: " + wasExecutionTrigerred());
            // check if we should redistribute tasks to include the new agent
            if (wasExecutionTrigerred()) {
                announceRebalancing();
            } else {

                String agentIp = request.params(REGISTRATION_PARAM).replaceFirst(":", "");
                agents.put(AGENT_PREFIX_NAME + id.getAndIncrement(), agentIp);


                System.out.println("Registered agent with ip " + agentIp);
                LOG.log(Level.INFO, "Registered agent with ip " + agentIp);
            }

            return "";
        });

        /* we should periodically query the agents to compute the total number of executions/tests
         * executed by all the agents. This serves as a heartbeat query as well, detecting the agents
         * that are no longer reachable in the cluster.
         */
        heartbeatScheduler.scheduleAtFixedRate(() ->
        {
                // System.out.println("heartbeat runs " + System.currentTimeMillis());
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
                        responseCode = executeHeartbeatRequest(heartBeatHttpClient, heartbeatRequest, agentId);
                        retrial -= 1;
                    }

                    if (retrial <= 0) {
                        LOG.log(Level.INFO, "Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                        System.out.println("Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
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

                System.out.println("[heartbeat scheduler] Executions per test: " + getExecutionsPerTest().toString());
                System.out.println("SIZE: " + runningTasks.keySet().size());

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

            System.out.println("AM FACUT CLEAR");
            // reset map of running tasks
            //runningTasks.clear();
        }
    }
}
