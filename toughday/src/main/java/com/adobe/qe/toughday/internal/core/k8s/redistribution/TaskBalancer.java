package com.adobe.qe.toughday.internal.core.k8s.redistribution;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.k8s.HttpUtils;
import com.adobe.qe.toughday.internal.core.k8s.cluster.K8SConfig;
import com.adobe.qe.toughday.internal.core.k8s.splitters.PhaseSplitter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import java.util.*;
import java.util.concurrent.*;

/**
 * Class responsible for balancing the work between the agents running the cluster
 * whenever the number of agents is changing.
 */
public class TaskBalancer {
    private static final String REBALANCE_PATH = "/rebalance";

    private final PhaseSplitter phaseSplitter = new PhaseSplitter();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private final HttpUtils httpUtils = new HttpUtils();
    private final ScheduledExecutorService rebalanceScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentLinkedQueue<String> inactiveAgents = new ConcurrentLinkedQueue<>();
    private ConcurrentHashMap<String, String> recentlyAddedAgents = new ConcurrentHashMap<>();

    private static TaskBalancer instance = null;
    private RebalanceState state = RebalanceState.UNNECESSARY;

    public enum RebalanceState {
        UNNECESSARY,
        SCHEDULED,
        RESCHEDULED_REQUIRED,
        EXECUTING
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

    private void updateCountPerTest(Phase phase, Map<String, Long> executionsPerTest) {
        phase.getTestSuite().getTests().forEach(test -> {
            long remained = test.getCount() - executionsPerTest.get(test.getName());
            if (remained < 0) {
                // set this to 0 so that the agents will know to delete the test from the test suite
                test.setCount("0");
            } else {
                test.setCount(String.valueOf(remained));
            }
        });
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
                String agentURI = "http://" + entry.getValue() + ":4567" + REBALANCE_PATH;
                TestSuite testSuite = phases.get(agentName).getTestSuite();
                RunMode runMode = phases.get(agentName).getRunMode();

                // set new value for count property
                Map<String, Long> testSuiteProperties = getTestSuitePropertiesToRedistribute(testSuite);
                Map<String, String> runModeProperties = runMode.getRunModeBalancer()
                        .getRunModePropertiesToRedistribute(runMode.getClass(), runMode);

                RedistributionInstructions redistributionInstructions = new RedistributionInstructions(testSuiteProperties, runModeProperties);
                try {
                    String instructionMessage = mapper.writeValueAsString(redistributionInstructions);
                    System.out.println("[rebalancing] Sending " + instructionMessage + " to agent " + agentName + ". Ip: " + entry.getValue());

                    this.httpUtils.sendSyncHttpRequest(instructionMessage, agentURI);

                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

        });
    }

    private void sendExecutionQueriesToNewAgents(Map<String, String> recentlyAddedAgents, Map<String, Phase> phases,
                                                 Configuration configuration,
                                                 Map<String, Future<HttpResponse>> newRunningTasks) {
        // for the recently added agents, send execution queries
        recentlyAddedAgents.forEach((newAgentName, newAgentIp) -> {
            configuration.setPhases(Collections.singletonList(phases.get(newAgentName)));
            YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
            String yamlTask = dumpConfig.generateConfigurationObject();
            String URI = "http://" + newAgentIp + ":4567" + "/submitTask";

            System.out.println("[task balancer] sending execution request + " + yamlTask + " to new agent " + newAgentName);
            Future<HttpResponse> future  =
                    this.httpUtils.sendAsyncHttpRequest(URI, yamlTask, this.asyncClient);
            newRunningTasks.put(newAgentName, future);

        });
    }

    private Map<String, Future<HttpResponse>> requestRebalancing
            (Phase phase, Map<String, Long> executionsPerTest,
            ConcurrentHashMap<String, String> activeAgents,
            Map<String, String> recentlyAddedAgents,
            Configuration configuration, long phaseStartTime) throws CloneNotSupportedException {
        // check if one agent failed to respond to heartbeat before TD execution started.
        if (phase == null) {
            return new HashMap<>();
        }
        // start by updating the number of tests that are left to be executed by the agents
        updateCountPerTest(phase, executionsPerTest);

        Map<String, Future<HttpResponse>> newRunningTasks = new HashMap<>();

        System.out.println("[task balancer] calling split phase for rebalancing work...");
        Map<String, Phase> phases = this.phaseSplitter.splitPhaseForRebalancingWork(phase, new ArrayList<>(activeAgents.keySet()),
                new ArrayList<>(recentlyAddedAgents.keySet()), phaseStartTime);

        System.out.println("[rebalancing]Size of agents : " + activeAgents.keySet().size() + " " + activeAgents.keySet().toString());
        sendInstructionsToOldAgents(phases, activeAgents);
        sendExecutionQueriesToNewAgents(recentlyAddedAgents, phases, configuration, newRunningTasks);

        return newRunningTasks;
    }

    private void excludeInactiveAgents(List<String> agentsToBeExcluded, ConcurrentHashMap<String, String> activeAgents,
                                       DistributedPhaseMonitor phaseMonitor) {
        System.out.println("[task balancer] agents to be excluded " + agentsToBeExcluded.toString());
        agentsToBeExcluded.forEach(activeAgents::remove);
        agentsToBeExcluded.forEach(this.inactiveAgents::remove);

        /* the number of tests executed by the agents which are now excluded should not be taken into consideration in
         * the future
         */
        agentsToBeExcluded.forEach(phaseMonitor::removeAgentFromExecutionsMap);

        System.out.println("[task balancer] executions per test after excluding agents " +
                phaseMonitor.getExecutionsPerTest().toString());
        System.out.println("[task balancer] active agents after excluding agents " + activeAgents.toString());

    }

    public Map<String, Future<HttpResponse>> rebalanceWork(DistributedPhaseMonitor phaseMonitor,
                                                           ConcurrentHashMap<String, String> activeAgents,
                                                           Configuration configuration,
                                                           K8SConfig k8SConfig, long phaseExecutionStartTime) {
        this.state = RebalanceState.EXECUTING;
        Map<String, Future<HttpResponse>> newRunningTasks = null;

        Map<String, String> newAgents = new HashMap<>(recentlyAddedAgents);
        List<String> inactiveAgents = new ArrayList<>(this.inactiveAgents);

        System.out.println("[Task Balancer] Inactive agents: " + inactiveAgents.toString());
        Map<String, Long> executionsPerTest = phaseMonitor.getExecutionsPerTest();
        System.out.println("[task balancer] executions per test " + executionsPerTest.toString());
        // remove all agents who failed answering the heartbeat request in the past

        excludeInactiveAgents(inactiveAgents, activeAgents, phaseMonitor);
        System.out.println("[Rebalance] Starting....");

        try {
           newRunningTasks = requestRebalancing(phaseMonitor.getPhase(), executionsPerTest, activeAgents,
                   newAgents, configuration, phaseExecutionStartTime);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        phaseMonitor.resetExecutions();
        System.out.println("[Rebalance] done rebalancing. Executions is " + phaseMonitor.getExecutionsPerTest().toString());
        // mark recently added agents as active agents executing tasks
        activeAgents.putAll(newAgents);
        newAgents.forEach((key, value) -> this.recentlyAddedAgents.remove(key));

        if (this.state == RebalanceState.RESCHEDULED_REQUIRED) {
            this.state = RebalanceState.SCHEDULED;
            System.out.println("[task balancer] delayed rebalance must be scheduled for new agents = " + this.recentlyAddedAgents.toString());
            this.rebalanceScheduler.schedule(() -> {
                System.out.println("[task balancer] starting delayed rebalancing...");
                System.out.println("[task balancer] seconds waited: " + k8SConfig.getRedistributionWaitTime());
                return rebalanceWork(phaseMonitor, activeAgents, configuration, k8SConfig, phaseExecutionStartTime);
            }, k8SConfig.getRedistributionWaitTimeInSeconds(), TimeUnit.SECONDS);
        } else {
            this.state = RebalanceState.UNNECESSARY;
        }

        return newRunningTasks;
    }

}
