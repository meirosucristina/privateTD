package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.MasterHeartbeatTask;
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
import spark.Spark;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
    private static final String MASTER_ELECTION_PATH = "/masterElection";
    private static final String VOTE_MASTER = "/voteMaster";
    private static final String ANNOUNCE_MASTER_PATH = "/announceMaster";

    public static final String SVC_NAME = "driver";
    private static final String SVC_PORT = "80";
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final HttpUtils httpUtils = new HttpUtils();
    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();
    private DistributedPhaseMonitor distributedPhaseMonitor = new DistributedPhaseMonitor();
    private Configuration configuration;
    private final Object object = new Object();
    private DriverState driverState;

    public Driver(Configuration configuration) {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            this.driverState = new DriverState(hostname, configuration);
        } catch (UnknownHostException e) {
            System.exit(-1);
        }
    }

    public static String getAgentRegisterPath() {
        return URL_PREFIX + SVC_NAME + ":" + SVC_PORT + REGISTER_PATH;
    }

    private static String getAgentRegisterPath(String driverHostname, boolean forwardReq) {
        return URL_PREFIX + driverHostname + ":4567" + REGISTER_PATH + "?forward=" + forwardReq;

    }

    public static String getPhaseFinishedByAgentPath() {
        return URL_PREFIX + SVC_NAME + ":" + SVC_PORT + PHASE_FINISHED_BY_AGENT_PATH;
    }

    public static String getSampleContentAckPath() {
        return URL_PREFIX + SVC_NAME + ":" + SVC_PORT + SAMPLE_CONTENT_ACK_PATH;
    }

    public static String getHealthPath(String driverHostName) {
        return URL_PREFIX + driverHostName + ":4567" + HEALTH_PATH;
    }

    private void masterElection() {
        int choice = Integer.MAX_VALUE;

        for (int i = this.driverState.getId(); i < this.driverState.getNrDrivers(); i++) {
            // skip inactive drivers
            if (this.driverState.getInactiveDrivers().contains(i)) {
                continue;
            }

            // chose the minimum id as master
            if (i < choice) {
                choice = i;
            }

            // TODO: solve this corner case later
            /*// announce all active drivers that the master election process should be triggered
            HttpResponse httpResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, "", MASTER_ELECTION_PATH,
                    HTTP_REQUEST_RETRIES);
            if (httpResponse == null) {
                // the assumption is that the driver died and he will received the id of the new master once restarted
                LOG.warn("Failed to inform driver " + SVC_NAME + "-" + i + " that master election process should be" +
                        "triggered.");
            }*/

            LOG.info("Driver " + this.driverState.getHostname() + " picked " + SVC_NAME + "-" + choice + " as the new master.");
            this.driverState.setCurrentState(DriverState.State.PICKED_MASTER);

            // send vote
            HttpResponse driverResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, "", VOTE_MASTER, HTTP_REQUEST_RETRIES);
            if (driverResponse != null) {
                break;
            }

            LOG.info("Failed to sent master vote to driver " +  SVC_NAME + "-" + choice + ". Restarting the election process...");
        }

        /*// wait until all the other drivers finished executing the master election process
        boolean finished = false;
        Queue<Integer> inactive = new LinkedList<>(this.driverState.getInactiveDrivers());
        for (int i = 0; i < this.driverState.getNrDrivers(); i++) {
            if (i == this.driverState.getId() || inactive.contains(i)) {
                continue;
            }

            // ping driver to ask for his state
            HttpResponse driverResponse = this.httpUtils.sendHttpRequest(HttpUtils.GET_METHOD, "", GET_DRIVER_STATE_PATH,
                    HTTP_REQUEST_RETRIES);
            String state =  EntityUtils.toString(driverResponse.getEntity());
            if (driverResponse != null && !state.equals(DriverState.State.PICKED_MASTER)) {

            }
        }*/

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
        List<String> agentsCopy = new ArrayList<>(this.driverState.getRegisteredAgents());

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
        if (this.driverState.getDriverConfig().getDistributedConfig().getHeartbeatIntervalInSeconds() ==
            this.configuration.getDistributedConfig().getHeartbeatIntervalInSeconds()) {

            this.driverState.getDriverConfig().getDistributedConfig().merge(configuration.getDistributedConfig());
            return;
        }

        // cancel heartbeat task and reschedule it with the new period
        this.heartbeatScheduler.shutdownNow();
        this.driverState.getDriverConfig().getDistributedConfig().merge(configuration.getDistributedConfig());
        scheduleHeartbeatTask();
    }

    private void finishAgents() {
        this.driverState.getRegisteredAgents().forEach(agentIp -> {
            LOG.info("[Driver] Finishing agent " + agentIp);
            HttpResponse response =
                    httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, "", Agent.getFinishPath(agentIp), HTTP_REQUEST_RETRIES);
            if (response == null) {
                // the assumption is that the agent will be killed when he fails to respond to heartbeat request
                LOG.warn("Driver could not finish the execution on agent " + agentIp + ".");
            }
        });

        this.driverState.getRegisteredAgents().clear();
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
                Map<String, Phase> tasks = phaseSplitter.splitPhase(phase, new ArrayList<>(this.driverState.getRegisteredAgents()));
                this.distributedPhaseMonitor.setPhase(phase);

                for (String agentIp : this.driverState.getRegisteredAgents()) {
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
        heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(this.driverState.getRegisteredAgents(), this.distributedPhaseMonitor,
                        this.configuration, this.driverState.getDriverConfig()),
                0, this.driverState.getDriverConfig().getDistributedConfig().getHeartbeatIntervalInSeconds(), TimeUnit.SECONDS);
    }

    private void scheduleMasterHeartbeatTask() {
        // we should periodically send heartbeat messages from slaves to check id the master is still running
        this.heartbeatScheduler.scheduleAtFixedRate(new MasterHeartbeatTask(this.driverState), 0,
                GlobalArgs.parseDurationToSeconds("5s"), TimeUnit.SECONDS);
    }

    public void run() {

        /* expose http endpoint the announce the new master */
        post(ANNOUNCE_MASTER_PATH, ((request, response) -> {
            int choice = Integer.parseInt(request.body());
            LOG.info(this.driverState.getHostname() + " was announced that driver-" + choice + " was chosen to be the master.");

            // update current state
            if (choice == this.driverState.getId()) {
                this.driverState.setCurrentState(DriverState.State.MASTER);
            } else {
                this.driverState.setCurrentState(DriverState.State.SLAVE);
            }

            this.driverState.resetNrVotes();
            return "";
        }));

        post(VOTE_MASTER, ((request, response) -> {
            this.driverState.increaseNumberOfVotes();
            LOG.info(this.driverState.getHostname() + " was voted. Total number of votes " + this.driverState.getNrVotes());

            // TODO: maybe compute number of active agents by pinging all agents and count the ones answering
            if (this.driverState.getNrVotes() >= this.driverState.getNrDrivers() / 2 + 1) {
                LOG.info(this.driverState.getHostname() + " : " + "announcing drivers that I am the new master");

                // announce this driver as the new master
                for (int i = 0; i < this.driverState.getNrDrivers(); i++) {
                    if (this.driverState.getInactiveDrivers().contains(i)) {
                        continue;
                    }

                    HttpResponse httpResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, String.valueOf(i), ANNOUNCE_MASTER_PATH,
                            HTTP_REQUEST_RETRIES);
                    if (httpResponse == null) {
                        LOG.error("Unable to announce driver " + SVC_NAME + "-" + i + " who is the new master.");
                        // TODO: treat this case later
                    }
                }
            }

            return "";
        }));

        /* expose http endpoint to announce driver that the master election process should be triggered */
        /*post(MASTER_ELECTION_PATH, ((request, response) -> {
            if (this.driverState.getCurrentState() == DriverState.State.CHOOSING_MASTER) {
                return "";
            }

            this.driverState.setCurrentState(DriverState.State.CHOOSING_MASTER);
            // mark current master as failed
            this.driverState.addInactiveDriver(this.driverState.getMasterId());
            // choose a new master
            masterElection();
            return "";
        }));*/

        /* expose http endpoint for running TD with the given configuration */
        post(EXECUTION_PATH, ((request, response) -> {
            // only master should trigger the execution
            if (this.driverState.getCurrentState() != DriverState.State.MASTER) {
                return "";
            }

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

        Spark.get(HEALTH_PATH, ((request, response) -> "Healthy"));

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

            if (!request.queryParams().contains("forward") || !request.queryParams("forward").equals("false")) {
                /* register new agents to all the drivers running in the cluster */
                for (int i = 0; i < this.driverState.getNrDrivers(); i++) {
                    /* skip current driver and inactive drivers */
                    if (i == this.driverState.getId() || this.driverState.getInactiveDrivers().contains(i)) {
                        continue;
                    }

                    LOG.info(this.driverState.getHostname() + ": sending agent register request for agent " + agentIp + "" +
                            "to driver " + this.driverState.getPathForId(i));
                    HttpResponse regResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, agentIp,
                            getAgentRegisterPath(this.driverState.getPathForId(i), false), HTTP_REQUEST_RETRIES);
                    if (regResponse == null) {
                        // the assumption is that the new driver will receive the full list of active agents after being restarted
                        LOG.info("Driver " + this.driverState.getHostname() + "failed to send register request for agent " + agentIp +
                                "to driver " + this.driverState.getPathForId(i));
                    }
                }
            }

            if (!this.distributedPhaseMonitor.isPhaseExecuting()) {
                this.driverState.registerAgent(agentIp);
                LOG.info("[driver] active agents " + this.driverState.getRegisteredAgents().toString());
                return "";
            }

            this.taskBalancer.rebalanceWork(distributedPhaseMonitor, this.driverState.getRegisteredAgents(), configuration,
                                            this.driverState.getDriverConfig().getDistributedConfig(), agentIp, true);

            return "";
        });

        if (this.driverState.getCurrentState() == DriverState.State.MASTER) {
            scheduleHeartbeatTask();
        } else if (this.driverState.getCurrentState() == DriverState.State.SLAVE) {
            scheduleMasterHeartbeatTask();
        }
    }
}
