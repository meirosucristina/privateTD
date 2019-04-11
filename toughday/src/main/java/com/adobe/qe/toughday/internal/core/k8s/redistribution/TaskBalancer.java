package com.adobe.qe.toughday.internal.core.k8s.redistribution;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.HttpUtils;
import com.adobe.qe.toughday.internal.core.k8s.splitters.PhaseSplitter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import java.lang.reflect.InvocationTargetException;
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
    private final ScheduledExecutorService scheduleRebalance = Executors.newSingleThreadScheduledExecutor();
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

    private Map<String, String> getRunModePropertiesToRedistribute(Class type, Object object) {
        final Map<String, String> properties = new HashMap<>();

        Arrays.stream(type.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(ConfigArgGet.class))
                .filter(method -> method.getAnnotation(ConfigArgGet.class).redistribute())
                .forEach(method -> {
                    try {
                        String propertyName = Configuration.propertyFromMethod(method.getName());
                        Object value = method.invoke(object);

                        properties.put(propertyName, String.valueOf(value));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                });

        return properties;
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
                // phase.getTestSuite().remove(test.getName());
            } else {
                test.setCount(String.valueOf(remained));
            }
        });
    }

    public void addNewAgent(String agentName, String ipAddress) {
        this.recentlyAddedAgents.put(agentName, ipAddress);
    }

    public Map<String, String> getRecentlyAddedAgents() {
        return this.recentlyAddedAgents;
    }

    public void addInactiveAgent(String agentName) {
        this.inactiveAgents.add(agentName);
    }

    public ConcurrentLinkedQueue<String> getInactiveAgents() {
        return this.inactiveAgents;
    }

    private void sendInstructionsToOldAgents(Map<String, Phase> phases,
                                             ConcurrentHashMap<String, String> activeAgents) {
        ObjectMapper mapper = new ObjectMapper();

        // convert each phase into instructions for olg agents to update their configuration
        activeAgents.forEach((agentName, agentIp) -> {
            String agentURI = "http://" + activeAgents.get(agentName) + ":4567" + REBALANCE_PATH;
            TestSuite testSuite = phases.get(agentName).getTestSuite();
            RunMode runMode = phases.get(agentName).getRunMode();

            // set new value for count property
            Map<String, Long> testSuiteProperties = getTestSuitePropertiesToRedistribute(testSuite);
            Map<String, String> runModeProperties = this.getRunModePropertiesToRedistribute(runMode.getClass(), runMode);

            RebalanceInstructions rebalanceInstructions = new RebalanceInstructions(testSuiteProperties, runModeProperties);
            try {
                String instructionMessage = mapper.writeValueAsString(rebalanceInstructions);
                System.out.println("[rebalancing] Sending " + instructionMessage + " to agent " + agentName + ". Ip: " + agentIp);

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
            Configuration configuration) throws CloneNotSupportedException {
        // check if one agent failed to respond to heartbeat before TD execution started.
        if (phase == null) {
            return new HashMap<>();
        }
        // start by updating the number of tests that are left to be executed by the agents
        updateCountPerTest(phase, executionsPerTest);

        Map<String, Future<HttpResponse>> newRunningTasks = new HashMap<>();

        Map<String, Phase> phases = this.phaseSplitter.splitPhaseForRebalancingWork(phase, new ArrayList<>(activeAgents.keySet()),
                new ArrayList<>(recentlyAddedAgents.keySet()));

        // System.out.println("[rebalancing]Size of agents : " + agentNames.size() + " : " + agentNames.toString());
        sendInstructionsToOldAgents(phases, activeAgents);
        sendExecutionQueriesToNewAgents(recentlyAddedAgents, phases, configuration, newRunningTasks);

        return newRunningTasks;
    }

    public Map<String, Future<HttpResponse>> rebalanceWork(Phase phase, Map<String, Long> executionsPerTest,
                              ConcurrentHashMap<String, String> activeAgents,
                              Configuration configuration) {
        this.state = RebalanceState.EXECUTING;
        Map<String, Future<HttpResponse>> newRunningTasks = null;
        Map<String, String> newAgents = new HashMap<>(recentlyAddedAgents);

        System.out.println("[Rebalance] Starting....");

        try {
           newRunningTasks = requestRebalancing(phase, executionsPerTest, activeAgents, newAgents, configuration);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        // mark recently added agents as active agents executing tasks
        activeAgents.putAll(newAgents);
        newAgents.forEach((key, value) -> this.recentlyAddedAgents.remove(key));

        if (this.state == RebalanceState.RESCHEDULED_REQUIRED) {
            this.state = RebalanceState.SCHEDULED;
            System.out.println("[task balancer] delayed rebalance must be scheduled for new agents = " + this.recentlyAddedAgents.toString());
            this.scheduleRebalance.schedule(() -> {
                System.out.println("[task balancer] starting delayed rebalancing...");
                return rebalanceWork(phase, executionsPerTest, activeAgents, configuration);
            }, GlobalArgs.parseDurationToSeconds("3s"), TimeUnit.SECONDS);
        } else {
            this.state = RebalanceState.UNNECESSARY;
        }

        return newRunningTasks;
    }

}
