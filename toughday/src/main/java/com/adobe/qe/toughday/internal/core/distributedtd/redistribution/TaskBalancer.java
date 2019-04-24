package com.adobe.qe.toughday.internal.core.distributedtd.redistribution;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.Agent;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseInfo;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.DistributedConfig;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.PhaseSplitter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final HttpUtils httpUtils = new HttpUtils();
    private final ScheduledExecutorService rebalanceScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentLinkedQueue<String> inactiveAgents = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<String> recentlyAddedAgents = new ConcurrentLinkedQueue<>();
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

    public void addNewAgent(String ipAddress) {
        this.recentlyAddedAgents.add(ipAddress);
    }

    public void addInactiveAgent(String agentName) {
        this.inactiveAgents.add(agentName);
    }

    private void sendInstructionsToOldAgents(Map<String, Phase> phases,
                                             Queue<String> activeAgents) {
        ObjectMapper mapper = new ObjectMapper();

        // convert each phase into instructions for olg agents to update their configuration
        activeAgents.stream()
            .filter(ipAddress -> !this.inactiveAgents.contains(ipAddress)) // filter agents that become inactive
            .forEach(ipAddress -> {
                String agentURI = Agent.getRebalancePath(ipAddress);
                TestSuite testSuite = phases.get(ipAddress).getTestSuite();
                RunMode runMode = phases.get(ipAddress).getRunMode();

                // set new value for count property
                Map<String, Long> testSuiteProperties = getTestSuitePropertiesToRedistribute(testSuite);
                Map<String, String> runModeProperties = runMode.getRunModeBalancer()
                        .getRunModePropertiesToRedistribute(runMode.getClass(), runMode);

                RedistributionInstructions redistributionInstructions =
                        new RedistributionInstructions(testSuiteProperties, runModeProperties);
                try {
                    String instructionMessage = mapper.writeValueAsString(redistributionInstructions);
                    LOG.info("[redistribution] Sending " + instructionMessage + " to agent " + ipAddress);
                    boolean successfullySentInstructions =
                            this.httpUtils.sendSyncHttpRequest(instructionMessage, agentURI);
                    if (!successfullySentInstructions) {
                        /* redistribution will be triggered as soon as this agent fails to respond to heartbeat
                         * request
                         */
                        LOG.warn("Redistribution instructions could not be sent to agent " + ipAddress + ". ");
                    }

                } catch (JsonProcessingException e) {
                    LOG.error("Unexpected exception while sending redistribution instruction", e);
                    LOG.warn("Agent " + ipAddress + " will continue to run with the configuration it had before " +
                            "the process of redistribution was triggered.");
                }

        });
    }

    private void sendExecutionQueriesToNewAgents(List<String> recentlyAddedAgents,
                                                 Map<String, Phase> phases,
                                                 Configuration configuration) {
        // for the recently added agents, send execution queries
        recentlyAddedAgents.forEach(newAgentIpAddress -> {
            configuration.setPhases(Collections.singletonList(phases.get(newAgentIpAddress)));
            YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
            String yamlTask = dumpConfig.generateConfigurationObject();

            LOG.info("[redistribution] sending execution request + " + yamlTask + " to new agent " + newAgentIpAddress);
            boolean successfullySentRequest = this.httpUtils
                    .sendSyncHttpRequest(yamlTask, Agent.getSubmissionTaskPath(newAgentIpAddress),  3);

            if (!successfullySentRequest) {
                LOG.info("Failed to send task to new agent " + newAgentIpAddress + ".");
                // TODO schedule redistribution
            }

        });
    }

    private void requestRebalancing(DistributedPhaseInfo distributedPhaseInfo,
                                    Queue<String> activeAgents, List<String> recentlyAddedAgents,
                                    Configuration configuration) throws CloneNotSupportedException {
        Phase phase = distributedPhaseInfo.getPhase();

        // check if one agent failed to respond to heartbeat before TD execution started.
        if (phase == null) {
            return;
        }

        // start by updating the number of tests that are left to be executed by the agents
        distributedPhaseInfo.updateCountPerTest();
        Map<String, Phase> phases = this.phaseSplitter.splitPhaseForRebalancingWork(phase, new ArrayList<>(activeAgents),
                new ArrayList<>(recentlyAddedAgents), distributedPhaseInfo.getPhaseStartTime());

        sendInstructionsToOldAgents(phases, activeAgents);
        sendExecutionQueriesToNewAgents(recentlyAddedAgents, phases, configuration);

    }

    private void excludeInactiveAgents(List<String> agentsToBeExcluded, Queue<String> activeAgents,
                                       DistributedPhaseInfo distributedPhaseInfo) {
        LOG.info("[redistribution] agents to be excluded " + agentsToBeExcluded.toString());
        agentsToBeExcluded.forEach(activeAgents::remove);
        agentsToBeExcluded.forEach(this.inactiveAgents::remove);
        agentsToBeExcluded.forEach(distributedPhaseInfo::removeAgentFromExecutionsMap);
    }

    public void rebalanceWork(DistributedPhaseInfo distributedPhaseInfo,
                              Queue<String> activeAgents,
                              Configuration configuration,
                              DistributedConfig distributedConfig) {
        this.state = RebalanceState.EXECUTING;

        List<String> newAgents = new ArrayList<>(recentlyAddedAgents);
        List<String> inactiveAgents = new ArrayList<>(this.inactiveAgents);

        // remove all agents who failed answering the heartbeat request in the past
        excludeInactiveAgents(inactiveAgents, activeAgents, distributedPhaseInfo);
        LOG.info("[Redistribution] Starting....");

        try {
          requestRebalancing(distributedPhaseInfo, activeAgents, newAgents, configuration);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        distributedPhaseInfo.resetExecutions();
        LOG.info("[Redistribution] Finished redistributing the work");

        // mark recently added agents as active agents executing tasks
        activeAgents.addAll(newAgents);
        newAgents.forEach(distributedPhaseInfo::registerAgentRunningTD);
        newAgents.forEach(ipAddress -> this.recentlyAddedAgents.remove(ipAddress));

        if (this.state == RebalanceState.RESCHEDULED_REQUIRED) {
            this.state = RebalanceState.SCHEDULED;
            LOG.info("[driver] Scheduling delayed redistribution process for agents " + this.recentlyAddedAgents.toString());
            this.rebalanceScheduler.schedule(() -> {
                LOG.info("[Redistribution] starting delayed work redistribution process");
                rebalanceWork(distributedPhaseInfo, activeAgents, configuration, distributedConfig);
            }, distributedConfig.getRedistributionWaitTimeInSeconds(), TimeUnit.SECONDS);
        } else {
            this.state = RebalanceState.UNNECESSARY;
        }

    }

}
