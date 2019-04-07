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
import static com.adobe.qe.toughday.internal.core.k8s.HttpUtils.*;
import static spark.Spark.*;

/**
 * Driver component for K8s cluster.
 */
public class Driver {

    private static final String PORT = "4567";
    private static final String REGISTRATION_PARAM = ":ipAddress";
    private static final String REGISTER_PATH = "/registerAgent/" + REGISTRATION_PARAM;

    protected static final Logger LOG = LogManager.getLogger(Agent.class);
    private static final AtomicInteger id = new AtomicInteger(0);
    private long heartbeatInterval = GlobalArgs.parseDurationToSeconds("5s");

    private final ConcurrentHashMap<String, String> agents = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private final Map<String, Future<HttpResponse>> runningTasks = new HashMap<>();
    private Map<String, String> recentlyAddedAgents = new HashMap<>();
    // key = name of the test; value = map(key = name of the agent, value = nr of tests executed)
    private Map<String, Map<String, Long>> executions = new HashMap<>();

    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final TaskBalancer taskBalancer = new TaskBalancer();
    private Configuration configuration;
    private Phase currentPhase;
    private final HttpUtils httpUtils = new HttpUtils();

    public Driver() {
        asyncClient.start();
    }

    private Map<String, Long> getExecutionsPerTest() {
        Map<String, Long> executionsPerTest = new HashMap<>();

        this.executions.forEach((testName, executionsPerAgent) ->
                executionsPerTest.put(testName, executionsPerAgent.values().stream().mapToLong(x -> x).sum()));

        return executionsPerTest;
    }

    private boolean areTasksRunning() {
        return !this.runningTasks.isEmpty();
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

                this.currentPhase = phase;
                phase.getTestSuite().getTests().forEach(test -> executions.put(test.getName(), new HashMap<>()));

                agents.keySet().forEach(agentId -> {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentId)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    /* send query to agent and register running task */
                    String URI = URL_PREFIX + agents.get(agentId) + ":" + PORT + SUBMIT_TASK_PATH;
                    runningTasks.put(agentId, this.httpUtils.sendAsyncHttpRequest(URI, yamlTask, asyncClient));

                    LOG.log(Level.INFO, "Task was submitted to agent " + agents.get(agentId));
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
                    // recently added agents might not execute tests yet
                    if (doubleAgentCounts.isEmpty()) {
                        return responseCode;
                    }

                    this.executions.forEach((testName, executionsPerAgent) ->
                            this.executions.get(testName).put(agentName, doubleAgentCounts.get(testName).longValue()));
                    //System.out.println("Am primit de la " + agentName + doubleAgentCounts.toString());

                } catch (Exception e) {
                    System.out.println("Executions/test were not successfully received from " + this.agents.get(agentName));
                    e.printStackTrace();
                    System.out.println("error message " + e.getMessage());
                }
            } else {
                //System.out.println("Response code != 200 for agent " + agentName + ". Value = " + responseCode);
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
            this.configuration = configuration;

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
            String agentName = AGENT_PREFIX_NAME + id.getAndIncrement();

            if (areTasksRunning()) {
                this.recentlyAddedAgents.put(agentName, agentIp);

                Map<String, Future<HttpResponse>> newRunningTasks =
                        taskBalancer.rebalanceWork(this.currentPhase, getExecutionsPerTest(),
                                agents, this.recentlyAddedAgents, this.configuration);

                // start monitoring the new tasks
                runningTasks.putAll(newRunningTasks);

                // mark recently added agents as active task executors
                agents.putAll(recentlyAddedAgents);
                recentlyAddedAgents.clear();

                System.out.println("Registered agent with ip " + agentIp);
                return "";
            }

            agents.put(agentName, agentIp);
            System.out.println("Registered agent with ip " + agentIp);
            System.out.println("agents : " + agents.keySet().toString());
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
                    responseCode = executeHeartbeatRequest(heartBeatHttpClient, heartbeatRequest, agentId);
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
            System.out.println("[heartbeat] Total nr of executions " + this.getExecutionsPerTest().toString());
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
