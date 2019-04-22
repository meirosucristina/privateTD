package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseInfo;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.CheckAgentRunsToughday;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.HeartbeatTask;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.PhaseSplitter;
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
import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.*;
import static spark.Spark.*;

/**
 * Driver component for the cluster.
 */
public class Driver {

    private static final String REGISTRATION_PARAM = ":ipAddress";
    private static final String REGISTER_PATH = "/registerAgent/" + REGISTRATION_PARAM;

    protected static final Logger LOG = LogManager.getLogger(Engine.class);
    private static final AtomicInteger id = new AtomicInteger(0);

    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService monitoringAgentsScheduler = Executors.newSingleThreadScheduledExecutor();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private final HttpUtils httpUtils = new HttpUtils();

    private final ConcurrentHashMap<String, String> agents = new ConcurrentHashMap<>();
    private List<ScheduledFuture<Map<String, Future<HttpResponse>>>> newRunningTasks = new ArrayList<>();
    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();
    private DistributedPhaseInfo distributedPhaseInfo = new DistributedPhaseInfo();
    private Configuration configuration;
    private Configuration driverConfiguration;

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

    private void mergeDistributedConfigParams(Configuration configuration) {
        if (this.driverConfiguration.getDistributedConfig().getHeartbeatIntervalInSeconds() ==
            this.configuration.getDistributedConfig().getHeartbeatIntervalInSeconds()) {

            this.driverConfiguration.getDistributedConfig().merge(configuration.getDistributedConfig());
            return;
        }

        // cancel heartbeat task and reschedule it with the new period
        this.heartbeatScheduler.shutdownNow();

        this.driverConfiguration.getDistributedConfig().merge(configuration.getDistributedConfig());
        scheduleHeartbeatTask();
    }

    private void finishAgents() {
        agents.forEach((key, value) -> {
            httpUtils.sendSyncHttpRequest("", HttpUtils.getFinishPath(value), 3);
            LOG.info("[Driver] Finishing agent " + key);
        });
    }

    private void handleExecutionRequest(Configuration configuration) {
        handleToughdaySampleContent(configuration);
        mergeDistributedConfigParams(configuration);

        PhaseSplitter phaseSplitter = new PhaseSplitter();

        for (Phase phase : configuration.getPhases()) {
            try {
                Map<String, Phase> tasks = phaseSplitter.splitPhase(phase, new ArrayList<>(agents.keySet()));
                this.distributedPhaseInfo.setPhase(phase);

                for (String agentName : agents.keySet()) {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentName)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    /* send query to agent and register running task */
                    String URI = HttpUtils.getSubmissionTaskPath(agents.get(agentName));
                    this.distributedPhaseInfo
                            .registerRunningTask(agentName, this.httpUtils.sendAsyncHttpRequest(URI, yamlTask, asyncClient));

                    LOG.log(Level.INFO, "Task was submitted to agent " + agents.get(agentName));
                }

                // al execution queries were sent => set phase execution start time
                this.distributedPhaseInfo.setPhaseStartTime(System.currentTimeMillis());

                // we should wait until all agents complete the current tasks in order to execute phases sequentially
                this.distributedPhaseInfo.waitForPhaseCompletion();

                LOG.log(Level.INFO, "Phase " + phase.getName() + " finished execution successfully.");

            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }

        // finish all agents
        finishAgents();
    }

    private void scheduleHeartbeatTask() {
        // we should periodically send heartbeat messages from driver to all the agents
        heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(this.agents, this.distributedPhaseInfo,
                        this.configuration, this.driverConfiguration),
                0, this.driverConfiguration.getDistributedConfig().getHeartbeatIntervalInSeconds(), TimeUnit.SECONDS);
    }

    private void scheduleAgentExecutionTrackerTask() {
        /* we should periodically check if recently added agents started executing tasks and
        we should monitor them.
         */
        this.monitoringAgentsScheduler.scheduleAtFixedRate(new CheckAgentRunsToughday(this.newRunningTasks, this.distributedPhaseInfo),
                0, GlobalArgs.parseDurationToSeconds("5s"), TimeUnit.SECONDS);
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
            if (!this.distributedPhaseInfo.isPhaseExecuting()) {
                agents.put(agentName, agentIp);
                LOG.info("[driver] active agents " + agents.keySet());
                return "";
            }

            taskBalancer.addNewAgent(agentName, agentIp);

            if (this.taskBalancer.getState() == TaskBalancer.RebalanceState.EXECUTING) {
                // work redistribution will be delayed until the current redistribution process is finished
                LOG.info("[driver] Delay redistribution process for agent " + agentName);
                this.taskBalancer.setState(TaskBalancer.RebalanceState.RESCHEDULED_REQUIRED);
                this.taskBalancer.addNewAgent(agentName, agentIp);

            } else if (this.taskBalancer.getState() != TaskBalancer.RebalanceState.SCHEDULED) {
                this.taskBalancer.setState(TaskBalancer.RebalanceState.SCHEDULED);
                LOG.info("[driver] Scheduling redistribution process to start in " +
                        configuration.getDistributedConfig().getRedistributionWaitTimeInSeconds() + " seconds.");

                // schedule redistribution process
                ScheduledFuture<Map<String, Future<HttpResponse>>> scheduledFuture =
                        this.taskBalancer.getRebalanceScheduler().schedule(() -> taskBalancer.rebalanceWork(
                                distributedPhaseInfo,
                                agents, configuration, driverConfiguration.getDistributedConfig(),
                                distributedPhaseInfo.getPhaseStartTime()),
                                configuration.getDistributedConfig().getRedistributionWaitTimeInSeconds(),
                                TimeUnit.SECONDS);
                newRunningTasks.add(scheduledFuture);
            }

            return "";
        });

        scheduleHeartbeatTask();

        scheduleAgentExecutionTrackerTask();

        /* wait for requests */
        while (true) { }
    }
}
