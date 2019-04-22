package com.adobe.qe.toughday.internal.core.distributedtd.redistribution;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseInfo;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.DistributedConfig;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.PhaseSplitter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

/**
 * Class responsible for balancing the work between the agents running the cluster
 * whenever the number of agents is changing.
 */
public class TaskBalancer {
    private final PhaseSplitter phaseSplitter = new PhaseSplitter();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private final HttpUtils httpUtils = new HttpUtils();
    private final ScheduledExecutorService rebalanceScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentLinkedQueue<String> inactiveAgents = new ConcurrentLinkedQueue<>();
    private ConcurrentHashMap<String, String> recentlyAddedAgents = new ConcurrentHashMap<>();
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private static TaskBalancer instance = null;
    private RebalanceState state = RebalanceState.UNNECESSARY;

    public enum RebalanceState {
        UNNECESSARY,            // no need to redistribute the work
        SCHEDULED,              // redistribution is scheduled
        RESCHEDULED_REQUIRED,   /* nr of agents running in the cluster has changed so we must reschedule the work
                                 * redistribution process */
        EXECUTING               // work redistribution process is executing right now
    }

    public ConcurrentLinkedQueue<String> getInactiveAgents() {
        return this.inactiveAgents;
    }

    public ScheduledExecutorService getRebalanceScheduler() {
        return this.rebalanceScheduler;
    }

    public void setState(RebalanceState state) {
        this.state = state;
    }

    public RebalanceState getState() {
        return this.state;
    }

    private TaskBalancer() {
        this.asyncClient.start();
    }

    public static TaskBalancer getInstance() {
        if (instance == null) {
            instance = new TaskBalancer();
        }

        return instance;
    }

    private Map<String, Long> getTestSuitePropertiesToRedistribute(TestSuite taskTestSuite) {
        HashMap<String, Long> map = new HashMap<>();
        taskTestSuite.getTests().forEach(test -> map.put(test.getName(), test.getCount()));

        return map;
    }

    public void addNewAgent(String agentName, String ipAddress) {
        this.recentlyAddedAgents.put(agentName, ipAddress);
    }

    public void addInactiveAgent(String agentName) {
        this.inactiveAgents.add(agentName);
    }

    private void sendInstructionsToOldAgents(Map<String, Phase> phases,
                                             ConcurrentHashMap<String, String> activeAgents) {
        ObjectMapper mapper = new ObjectMapper();

        // convert each phase into instructions for olg agents to update their configuration
        activeAgents.entrySet().stream()
            .filter(entry -> !this.inactiveAgents.contains(entry.getKey())) // filter agents that become inactive
            .forEach(entry -> {
                String agentName = entry.getKey();
                String agentURI = HttpUtils.getRebalancePath(entry.getValue());
                TestSuite testSuite = phases.get(agentName).getTestSuite();
                RunMode runMode = phases.get(agentName).getRunMode();

                // set new value for count property
                Map<String, Long> testSuiteProperties = getTestSuitePropertiesToRedistribute(testSuite);
                Map<String, String> runModeProperties = runMode.getRunModeBalancer()
                        .getRunModePropertiesToRedistribute(runMode.getClass(), runMode);

                RedistributionInstructions redistributionInstructions =
                        new RedistributionInstructions(testSuiteProperties, runModeProperties);
                try {
                    String instructionMessage = mapper.writeValueAsString(redistributionInstructions);
                    LOG.info("[redistribution] Sending " + instructionMessage + " to agent " + agentName +
                            "(" + entry.getValue() + ")");
                    this.httpUtils.sendSyncHttpRequest(instructionMessage, agentURI);

                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

        });
    }

    private Map<String, Future<HttpResponse>> sendExecutionQueriesToNewAgents(Map<String, String> recentlyAddedAgents,
                                                                              Map<String, Phase> phases,
                                                                              Configuration configuration) {
        Map<String, Future<HttpResponse>> newRunningTasks = new HashMap<>();

        // for the recently added agents, send execution queries
        recentlyAddedAgents.forEach((newAgentName, newAgentIp) -> {
            configuration.setPhases(Collections.singletonList(phases.get(newAgentName)));
            YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
            String yamlTask = dumpConfig.generateConfigurationObject();

            LOG.info("[redistribution] sending execution request + " + yamlTask + " to new agent " + newAgentName);
            Future<HttpResponse> future  =
                    this.httpUtils.sendAsyncHttpRequest(HttpUtils.getSubmissionTaskPath(newAgentIp), yamlTask, this.asyncClient);
            newRunningTasks.put(newAgentName, future);

        });

        return newRunningTasks;
    }

    private Map<String, Future<HttpResponse>> requestRebalancing
            (DistributedPhaseInfo distributedPhaseInfo,
            ConcurrentHashMap<String, String> activeAgents,
            Map<String, String> recentlyAddedAgents,
            Configuration configuration, long phaseStartTime) throws CloneNotSupportedException {
        Phase phase = distributedPhaseInfo.getPhase();

        // check if one agent failed to respond to heartbeat before TD execution started.
        if (phase == null) {
            return new HashMap<>();
        }

        // start by updating the number of tests that are left to be executed by the agents
        distributedPhaseInfo.updateCountPerTest();
        Map<String, Phase> phases = this.phaseSplitter.splitPhaseForRebalancingWork(phase, new ArrayList<>(activeAgents.keySet()),
                new ArrayList<>(recentlyAddedAgents.keySet()), phaseStartTime);

        sendInstructionsToOldAgents(phases, activeAgents);
        return sendExecutionQueriesToNewAgents(recentlyAddedAgents, phases, configuration);

    }

    private void excludeInactiveAgents(List<String> agentsToBeExcluded, ConcurrentHashMap<String, String> activeAgents,
                                       DistributedPhaseInfo distributedPhaseInfo) {
        LOG.info("[redistribution] agents to be excluded " + agentsToBeExcluded.toString());
        agentsToBeExcluded.forEach(activeAgents::remove);
        agentsToBeExcluded.forEach(this.inactiveAgents::remove);
        agentsToBeExcluded.forEach(distributedPhaseInfo::removeAgentFromExecutionsMap);
    }

    public Map<String, Future<HttpResponse>> rebalanceWork(DistributedPhaseInfo distributedPhaseInfo,
                                                           ConcurrentHashMap<String, String> activeAgents,
                                                           Configuration configuration,
                                                           DistributedConfig distributedConfig, long phaseExecutionStartTime) {
        this.state = RebalanceState.EXECUTING;
        Map<String, Future<HttpResponse>> newRunningTasks = null;

        Map<String, String> newAgents = new HashMap<>(recentlyAddedAgents);
        List<String> inactiveAgents = new ArrayList<>(this.inactiveAgents);

        // remove all agents who failed answering the heartbeat request in the past
        excludeInactiveAgents(inactiveAgents, activeAgents, distributedPhaseInfo);
        LOG.info("[Redistribution] Starting....");

        try {
           newRunningTasks = requestRebalancing(distributedPhaseInfo, activeAgents,
                   newAgents, configuration, phaseExecutionStartTime);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        distributedPhaseInfo.resetExecutions();
        LOG.info("[Redistribution] done redistributing the work");

        // mark recently added agents as active agents executing tasks
        activeAgents.putAll(newAgents);
        newAgents.forEach((key, value) -> this.recentlyAddedAgents.remove(key));

        if (this.state == RebalanceState.RESCHEDULED_REQUIRED) {
            this.state = RebalanceState.SCHEDULED;
            LOG.info("[driver] Delay redistribution process for agents " + this.recentlyAddedAgents.toString());
            this.rebalanceScheduler.schedule(() -> {
                LOG.info("[Redistribution] starting delayed work redistribution process");
                return rebalanceWork(distributedPhaseInfo, activeAgents, configuration, distributedConfig, phaseExecutionStartTime);
            }, distributedConfig.getRedistributionWaitTimeInSeconds(), TimeUnit.SECONDS);
        } else {
            this.state = RebalanceState.UNNECESSARY;
        }

        return newRunningTasks;
    }

}
