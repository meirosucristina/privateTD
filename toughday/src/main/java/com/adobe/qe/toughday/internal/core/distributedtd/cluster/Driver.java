package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.HeartbeatTask;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.PhaseSplitter;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.HTTP_REQUEST_RETRIES;
import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.URL_PREFIX;
import static com.adobe.qe.toughday.internal.core.engine.Engine.logGlobal;
import static spark.Spark.*;

/**
 * Driver component for the cluster.
 */
public class Driver {
    // routes
    public static final String EXECUTION_PATH = "/config";
    private static final String REGISTER_PATH = "/registerAgent";
    private static final String PHASE_FINISHED_BY_AGENT_PATH = "/phaseFinished";
    private static final String HEALTH_PATH = "/health";
    private static final String SAMPLE_CONTENT_ACK_PATH = "/contentAck";

    private static final String HOSTNAME = "driver";
    private static final String PORT = "80";
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final HttpUtils httpUtils = new HttpUtils();
    private final Queue<String> agents = new ConcurrentLinkedQueue<>();
    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();
    private DistributedPhaseMonitor distributedPhaseMonitor = new DistributedPhaseMonitor();
    private Configuration configuration;
    private Configuration driverConfiguration;
    private final Object object = new Object();

    public Driver(Configuration configuration) {
        this.driverConfiguration = configuration;
    }

    public static String getAgentRegisterPath() {
        return URL_PREFIX + HOSTNAME + ":" + PORT + REGISTER_PATH;
    }

    public static String getPhaseFinishedByAgentPath() {
        return URL_PREFIX + HOSTNAME + ":" + PORT + PHASE_FINISHED_BY_AGENT_PATH;
    }

    public static String getSampleContentAckPath() {
        return URL_PREFIX + HOSTNAME + ":" + PORT + SAMPLE_CONTENT_ACK_PATH;
    }

    private void waitForSampleContentToBeInstalled() {
        Future<?> future = executorService.submit(() -> {
            while (true) {
                try {
                    synchronized (object) {
                        object.wait();
                        break;
                    }
                } catch (InterruptedException e) {
                    // ignore and continue waiting for ack
                }
            }
        });

        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to install ToughDay sample content. Execution will be stopped.");
            finishDistributedExecution();
            System.exit(-1);
        }
    }

    private void handleToughdaySampleContent(Configuration configuration) {
        logGlobal("Installing ToughDay 2 Content Package...");
        GlobalArgs globalArgs = configuration.getGlobalArgs();

        if (globalArgs.getDryRun() || !globalArgs.getInstallSampleContent()) {
            return;
        }

        HttpResponse agentResponse = null;
        List<String> agentsCopy = new ArrayList<>(this.agents);

        while (agentResponse == null && agentsCopy.size() > 0) {
            // pick one agent to install the sample content
            String agentIpAddress = agentsCopy.remove(0);
            String URI = Agent.getInstallSampleContentPath(agentIpAddress);
            LOG.info("Installing sample content request was sent to agent " + agentIpAddress);

            YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
            String yamlTask = dumpConfig.generateConfigurationObject();

            agentResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, yamlTask, URI, HTTP_REQUEST_RETRIES);
        }

        if (agentResponse == null) {
            LOG.error("Failed to sent request to agents to install the sample content. Execution will be stopped.");
            finishDistributedExecution();
            System.exit(-1);
        }

        // we should wait until the we receive confirmation that the sample content was successfully installed
        waitForSampleContentToBeInstalled();

        logGlobal("Finished installing ToughDay 2 Content Package.");
        globalArgs.setInstallSampleContent("false");

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
        agents.forEach(agentIp -> {
            LOG.info("[Driver] Finishing agent " + agentIp);
            HttpResponse response =
                    httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, "", Agent.getFinishPath(agentIp), HTTP_REQUEST_RETRIES);
            if (response == null) {
                // the assumption is that the agent will be killed when he fails to respond to heartbeat request
                LOG.warn("Driver could not finish the execution on agent " + agentIp + ".");
            }
        });

        agents.clear();
    }

    private void finishDistributedExecution() {
        executorService.shutdownNow();

        // finish tasks
        this.heartbeatScheduler.shutdownNow();

        // finish agents
        this.finishAgents();
    }

    private void handleExecutionRequest(Configuration configuration) {
        handleToughdaySampleContent(configuration);
        mergeDistributedConfigParams(configuration);

        PhaseSplitter phaseSplitter = new PhaseSplitter();

        for (Phase phase : configuration.getPhases()) {
            try {
                Map<String, Phase> tasks = phaseSplitter.splitPhase(phase, new ArrayList<>(agents));
                this.distributedPhaseMonitor.setPhase(phase);

                for (String agentIp : agents) {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentIp)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    /* send query to agent and register running task */
                    String URI = Agent.getSubmissionTaskPath(agentIp);
                    HttpResponse response = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, yamlTask, URI, HTTP_REQUEST_RETRIES);

                    if (response != null) {
                        this.distributedPhaseMonitor.registerAgentRunningTD(agentIp);
                        LOG.info("Task was submitted to agent " + agentIp);
                    } else {
                        /* the assumption is that the agent is no longer active in the cluster and he will fail to respond
                         * to the heartbeat request sent by the driver. This will automatically trigger process of
                         * redistributing the work
                         * */
                        LOG.info("Task\n" + yamlTask + " could not be submitted to agent " + agentIp +
                                ". Work will be rebalanced once the agent fails to respond to heartbeat request.");
                    }
                }

                // al execution queries were sent => set phase execution start time
                this.distributedPhaseMonitor.setPhaseStartTime(System.currentTimeMillis());

                // we should wait until all agents complete the current tasks in order to execute phases sequentially
                if (!this.distributedPhaseMonitor.waitForPhaseCompletion(3)) {
                    break;
                }

                LOG.info("Phase " + phase.getName() + " finished execution successfully.");

            } catch (CloneNotSupportedException e) {
                LOG.error("Phase " + phase.getName() + " could not de divided into tasks to be sent to the agents.", e);

                LOG.info("Finishing agents");
                finishAgents();

                System.exit(-1);
            }
        }

        finishDistributedExecution();
    }

    private void scheduleHeartbeatTask() {
        // we should periodically send heartbeat messages from driver to all the agents
        heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(this.agents, this.distributedPhaseMonitor,
                        this.configuration, this.driverConfiguration),
                0, this.driverConfiguration.getDistributedConfig().getHeartbeatIntervalInSeconds(), TimeUnit.SECONDS);
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

        get(HEALTH_PATH, ((request, response) -> "Healthy"));

        post(SAMPLE_CONTENT_ACK_PATH, ((response, request) -> {
            LOG.info("Received ack from agent");
            synchronized (object) {
                object.notify();
            }

            return "";
        }));

        /* expose http endpoint to allow agents to announce when they finished executing the current phase */
        post(PHASE_FINISHED_BY_AGENT_PATH, ((request, response) -> {
            String agentIp = request.body();

            LOG.info("Agent " + agentIp + " finished executing the current phase.");
            this.distributedPhaseMonitor.removeAgentFromActiveTDRunners(agentIp);

            return "";
        }));

        /* expose http endpoint for registering new agents in the cluster */
        post(REGISTER_PATH, (request, response) -> {
            String agentIp = request.body();

            LOG.info("[driver] Registered agent with ip " + agentIp);
            if (!this.distributedPhaseMonitor.isPhaseExecuting()) {
                agents.add(agentIp);
                LOG.info("[driver] active agents " + agents.toString());
                return "";
            }

            this.taskBalancer.rebalanceWork(distributedPhaseMonitor, agents, configuration,
                                            driverConfiguration.getDistributedConfig(), agentIp, true);

            return "";
        });

        scheduleHeartbeatTask();
    }
}
