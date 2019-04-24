package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseInfo;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.HeartbeatTask;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.PhaseSplitter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

import static com.adobe.qe.toughday.internal.core.engine.Engine.installToughdayContentPackage;
import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.*;
import static spark.Spark.*;

/**
 * Driver component for the cluster.
 */
public class Driver {
    // routes
    public static final String EXECUTION_PATH = "/config";
    private static final String REGISTER_PATH = "/registerAgent";
    private static final String PHASE_FINISHED_BY_AGENT = "/phaseFinished";

    private static final String HOSTNAME = "driver";
    private static final int HTTP_REQUEST_RETRIES = 3;
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final HttpUtils httpUtils = new HttpUtils();
    private final Queue<String> agents = new ConcurrentLinkedQueue<>();
    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();
    private DistributedPhaseInfo distributedPhaseInfo = new DistributedPhaseInfo();
    private Configuration configuration;
    private Configuration driverConfiguration;

    public Driver(Configuration configuration) {
        this.driverConfiguration = configuration;
    }

    public static String getAgentRegisterPath() {
        return URL_PREFIX + HOSTNAME + ":80" + REGISTER_PATH;
    }

    public static String getPhaseFinishedByAgentPath() {
        return URL_PREFIX + HOSTNAME + ":80" + PHASE_FINISHED_BY_AGENT;
    }

    private void handleToughdaySampleContent(Configuration configuration) {
        GlobalArgs globalArgs = configuration.getGlobalArgs();

        if (globalArgs.getInstallSampleContent() && !globalArgs.getDryRun()) {
            try {
                installToughdayContentPackage(globalArgs);
                globalArgs.setInstallSampleContent("false");
            } catch (Exception e) {
                LOG.info("Failed to install sample content. Execution will pe stopped.");
                finishDistributedExecution();
                System.exit(-1);
            }
        }
    }

    private boolean waitForPhaseCompletion() {
        int retries = 3;

        while (retries > 0) {
            try {
                this.distributedPhaseInfo.waitForPhaseCompletion();
                return true;
            } catch (ExecutionException | InterruptedException e) {
                retries--;
            }
        }

        LOG.warn("Exception occurred while waiting for the completion of the current phase. The remaining " +
                "of the phases will no longer be executed");
        return false;
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
            httpUtils.sendSyncHttpRequest("", Agent.getFinishPath(agentIp), 3);
            LOG.info("[Driver] Finishing agent " + agentIp);
        });
    }

    private void finishDistributedExecution() {
        // finish agents
        this.finishAgents();

        // finish tasks
        this.heartbeatScheduler.shutdownNow();
    }

    private void handleExecutionRequest(Configuration configuration) {
        handleToughdaySampleContent(configuration);
        mergeDistributedConfigParams(configuration);

        PhaseSplitter phaseSplitter = new PhaseSplitter();

        for (Phase phase : configuration.getPhases()) {
            try {
                Map<String, Phase> tasks = phaseSplitter.splitPhase(phase, new ArrayList<>(agents));
                this.distributedPhaseInfo.setPhase(phase);

                for (String agentIp : agents) {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentIp)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    /* send query to agent and register running task */
                    String URI = Agent.getSubmissionTaskPath(agentIp);
                    boolean taskSuccessfullySent = this.httpUtils.sendSyncHttpRequest(yamlTask, URI, HTTP_REQUEST_RETRIES);
                    if (taskSuccessfullySent) {
                        this.distributedPhaseInfo.registerAgentRunningTD(agentIp);
                    } else {
                        LOG.info("Task\n" + yamlTask + " could not be submitted to agent " + agentIp +
                                ". Work will be rebalanced.");
                    }

                    LOG.info("Task was submitted to agent " + agentIp);
                }

                // al execution queries were sent => set phase execution start time
                this.distributedPhaseInfo.setPhaseStartTime(System.currentTimeMillis());

                // we should wait until all agents complete the current tasks in order to execute phases sequentially
                if (!waitForPhaseCompletion()) {
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
        heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(this.agents, this.distributedPhaseInfo,
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

        get("/health", ((request, response) -> "Healthy"));

        /* expose http endpoint to allow agents to announce when they finished executing the current phase */
        post(PHASE_FINISHED_BY_AGENT, ((request, response) -> {
            String agentIp = request.body();

            LOG.info("Agent " + agentIp + " finished executing the current phase.");
            this.distributedPhaseInfo.removeAgentFromActiveTDRunners(agentIp);

            return "";
        }));

        /* expose http endpoint for registering new agents in the cluster */
        post(REGISTER_PATH, (request, response) -> {
            String agentIp = request.body();

            LOG.info("[driver] Registered agent with ip " + agentIp);
            if (!this.distributedPhaseInfo.isPhaseExecuting()) {
                agents.add(agentIp);
                LOG.info("[driver] active agents " + agents.toString());
                return "";
            }

            taskBalancer.addNewAgent(agentIp);

            if (this.taskBalancer.getState() == TaskBalancer.RebalanceState.EXECUTING) {
                // work redistribution will be delayed until the current redistribution process is finished
                LOG.info("[driver] Delay redistribution process for agent " + agentIp);
                this.taskBalancer.setState(TaskBalancer.RebalanceState.RESCHEDULED_REQUIRED);
                this.taskBalancer.addNewAgent(agentIp);

            } else if (this.taskBalancer.getState() == TaskBalancer.RebalanceState.UNNECESSARY) {
                this.taskBalancer.setState(TaskBalancer.RebalanceState.SCHEDULED);
                LOG.info("[driver] Scheduling redistribution process to start in " +
                        configuration.getDistributedConfig().getRedistributionWaitTimeInSeconds() + " seconds.");

                // schedule redistribution process
                this.taskBalancer.getRebalanceScheduler().schedule(() -> taskBalancer.rebalanceWork(
                        distributedPhaseInfo,
                        agents, configuration, driverConfiguration.getDistributedConfig()),
                        configuration.getDistributedConfig().getRedistributionWaitTimeInSeconds(),
                        TimeUnit.SECONDS);
            }

            return "";
        });

        scheduleHeartbeatTask();

        /* wait for requests */
        while (true) { }
    }
}
