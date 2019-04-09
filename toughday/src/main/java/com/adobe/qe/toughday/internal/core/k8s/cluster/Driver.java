package com.adobe.qe.toughday.internal.core.k8s.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.k8s.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.k8s.HeartbeatTask;
import com.adobe.qe.toughday.internal.core.k8s.HttpUtils;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.TaskBalancer;
import com.adobe.qe.toughday.internal.core.k8s.splitters.PhaseSplitter;
import org.apache.http.HttpResponse;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    protected static final Logger LOG = LogManager.getLogger(Driver.class);
    private static final AtomicInteger id = new AtomicInteger(0);
    private long heartbeatInterval = GlobalArgs.parseDurationToSeconds("5s");

    private final ConcurrentHashMap<String, String> agents = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService rebalanceScheduler = Executors.newSingleThreadScheduledExecutor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private Map<String, String> recentlyAddedAgents = new HashMap<>();

    private final TaskBalancer taskBalancer = new TaskBalancer();
    private final HttpUtils httpUtils = new HttpUtils();
    private DistributedPhaseMonitor distributedPhaseMonitor = new DistributedPhaseMonitor();
    private Configuration configuration;

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

        PhaseSplitter phaseSplitter = new PhaseSplitter();

        for (Phase phase : configuration.getPhases()) {
            try {
                Map<String, Phase> tasks = phaseSplitter.splitPhase(phase, new ArrayList<>(agents.keySet()));
                this.distributedPhaseMonitor.setPhase(phase);

                for (String agentName : agents.keySet()) {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentName)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    /* send query to agent and register running task */
                    String URI = URL_PREFIX + agents.get(agentName) + ":" + PORT + SUBMIT_TASK_PATH;
                    this.distributedPhaseMonitor
                            .registerRunningTask(agentName, this.httpUtils.sendAsyncHttpRequest(URI, yamlTask, asyncClient));

                    LOG.log(Level.INFO, "Task was submitted to agent " + agents.get(agentName));
                }

                // we should wait until all agents complete the current tasks in order to execute phases sequentially
                this.distributedPhaseMonitor.waitForPhaseCompletion();

                LOG.log(Level.INFO, "Phase " + phase.getName() + " finished execution successfully.");

            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
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

            if (this.distributedPhaseMonitor.isPhaseExecuting()) {
                this.recentlyAddedAgents.put(agentName, agentIp);

                Map<String, Future<HttpResponse>> newRunningTasks =
                        taskBalancer.rebalanceWork(this.distributedPhaseMonitor.getPhase(),
                                this.distributedPhaseMonitor.getExecutionsPerTest(),
                                agents, this.recentlyAddedAgents, this.configuration);

                // start monitoring the new tasks
                newRunningTasks.forEach(distributedPhaseMonitor::registerRunningTask);

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

        System.out.println("Scheduling heartbeat...");
        // we should periodically send heartbeat messages from driver to all the agents
        heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(this.agents, this.distributedPhaseMonitor, this.configuration),
                0, heartbeatInterval, TimeUnit.SECONDS);

        /* wait for requests */
        while (true) { }
    }
}
