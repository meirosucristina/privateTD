package com.adobe.qe.toughday.internal.core.k8s.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
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
import java.util.stream.Collectors;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;


import static com.adobe.qe.toughday.internal.core.engine.Engine.installToughdayContentPackage;
import static com.adobe.qe.toughday.internal.core.k8s.HttpUtils.*;
import static spark.Spark.*;

/**
 * Driver component for K8s cluster.
 */
public class Driver {

    private static final String REGISTRATION_PARAM = ":ipAddress";
    private static final String REGISTER_PATH = "/registerAgent/" + REGISTRATION_PARAM;

    protected static final Logger LOG = LogManager.getLogger(Engine.class);
    private static final AtomicInteger id = new AtomicInteger(0);

    private final ConcurrentHashMap<String, String> agents = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService monitoringAgentsScheduler = Executors.newSingleThreadScheduledExecutor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();

    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();
    private final HttpUtils httpUtils = new HttpUtils();
    private DistributedPhaseMonitor distributedPhaseMonitor = new DistributedPhaseMonitor();
    private Configuration configuration;
    private Configuration driverConfiguration;
    private List<ScheduledFuture<Map<String, Future<HttpResponse>>>> newRunningTasks = new ArrayList<>();

    public Driver(Configuration configuration) {
        this.driverConfiguration = configuration;
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

    private void mergeK8sConfigParams(Configuration configuration) {
        if (this.driverConfiguration.getK8SConfig().getHeartbeatIntervalInSeconds() ==
            this.configuration.getK8SConfig().getHeartbeatIntervalInSeconds()) {

            this.driverConfiguration.getK8SConfig().merge(configuration.getK8SConfig());
            return;
        }

        // cancel heartbeat task and reschedule it with the new period
        this.heartbeatScheduler.shutdownNow();

        this.driverConfiguration.getK8SConfig().merge(configuration.getK8SConfig());
        scheduleHeartbeatTask();
    }

    private void handleExecutionRequest(Configuration configuration) {
        handleToughdaySampleContent(configuration);
        mergeK8sConfigParams(configuration);

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
                    String URI = HttpUtils.getSubmissionTaskPath(agents.get(agentName));
                    this.distributedPhaseMonitor
                            .registerRunningTask(agentName, this.httpUtils.sendAsyncHttpRequest(URI, yamlTask, asyncClient));

                    LOG.log(Level.INFO, "Task was submitted to agent " + agents.get(agentName));
                }

                // al execution queries were sent => set phase execution start time
                this.distributedPhaseMonitor.setPhaseStartTime(System.currentTimeMillis());

                // we should wait until all agents complete the current tasks in order to execute phases sequentially
                this.distributedPhaseMonitor.waitForPhaseCompletion();

                LOG.log(Level.INFO, "Phase " + phase.getName() + " finished execution successfully.");

            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }
    }

    private void scheduleHeartbeatTask() {
        // we should periodically send heartbeat messages from driver to all the agents
        heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(this.agents, this.distributedPhaseMonitor,
                        this.configuration, this.driverConfiguration),
                0, this.driverConfiguration.getK8SConfig().getHeartbeatIntervalInSeconds(), TimeUnit.SECONDS);
    }

    private void scheduleRecentlyRegisteredAgentsMonitoringTask() {
        /* we should periodically check if recently added agents started executing tasks and
        we should monitor them.
         */
        this.monitoringAgentsScheduler.scheduleAtFixedRate(() -> {
            System.out.println("[driver - monitoring agents] Started...");
            List<ScheduledFuture<Map<String, Future<HttpResponse>>>> finishedFutures = this.newRunningTasks.stream()
                    .filter(Future::isDone)
                    .collect(Collectors.toList());

            finishedFutures.forEach(future -> {
                System.out.println("[driver - monitoring agents] monitoring new running tasks has started...");
                try {
                    future.get().forEach(this.distributedPhaseMonitor::registerRunningTask);
                    this.newRunningTasks.removeAll(finishedFutures);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        }, 0, GlobalArgs.parseDurationToSeconds("5s"), TimeUnit.SECONDS);

    }

    public void run() {
        /* expose http endpoint for running TD with the given configuration */
        post(EXECUTION_PATH, ((request, response) -> {
            String yamlConfiguration = request.body();
            Configuration configuration = new Configuration(yamlConfiguration);
            this.configuration = configuration;

            // handle execution in a different thread to be able to quickly respond to this request
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

            LOG.info("[driver] Registered agent " + agentName + "with ip " + agentIp);
            if (!this.distributedPhaseMonitor.isPhaseExecuting()) {
                agents.put(agentName, agentIp);
                LOG.debug("[driver] active agents " + agents.keySet());
                return "";
            }

            taskBalancer.addNewAgent(agentName, agentIp);

            if (this.taskBalancer.getState() == TaskBalancer.RebalanceState.EXECUTING) {
                // work redistribution will be delayed until the current redistribution process is finished
                LOG.debug("[driver] Delay redistribution process for agent " + agentName);
                this.taskBalancer.setState(TaskBalancer.RebalanceState.RESCHEDULED_REQUIRED);
                this.taskBalancer.addNewAgent(agentName, agentIp);

            } else if (this.taskBalancer.getState() != TaskBalancer.RebalanceState.SCHEDULED) {
                this.taskBalancer.setState(TaskBalancer.RebalanceState.SCHEDULED);
                LOG.debug("[driver] Scheduling redistribution process to start in " +
                        configuration.getK8SConfig().getRedistributionWaitTimeInSeconds() + " seconds.");

                // schedule redistribution process
                ScheduledFuture<Map<String, Future<HttpResponse>>> scheduledFuture =
                        this.taskBalancer.getRebalanceScheduler().schedule(() -> taskBalancer.rebalanceWork(
                                distributedPhaseMonitor,
                                agents, configuration, driverConfiguration.getK8SConfig(),
                                distributedPhaseMonitor.getPhaseStartTime()),
                                configuration.getK8SConfig().getRedistributionWaitTimeInSeconds(),
                                TimeUnit.SECONDS);
                newRunningTasks.add(scheduledFuture);
            }

            return "";
        });

        scheduleHeartbeatTask();

        scheduleRecentlyRegisteredAgentsMonitoringTask();

        /* wait for requests */
        while (true) { }
    }
}
