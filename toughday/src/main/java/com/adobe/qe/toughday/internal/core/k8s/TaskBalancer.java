package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Class responsible for balancing the work between the agents running the cluster
 * whenever the number of agents is changing.
 */
public class TaskBalancer {
    private static final String REBALANCE_PATH = "/rebalance";

    private final PhasePartitioner phasePartitioner = new PhasePartitioner();
    private final CloseableHttpAsyncClient asyncClient = HttpAsyncClients.createDefault();
    private final HttpUtils httpUtils = new HttpUtils();

    public TaskBalancer() {
        this.asyncClient.start();
    }

    /* Contains all the information needed by the agents for updating their configuration when
     * the work needs to be rebalanced.
     */
    public static class RebalanceInstructions {
        private Map<String, Long> counts;
        private Map<String, String> runModeProperties;

        // dummy constructor, required for Jackson
        public RebalanceInstructions() {

        }

        public RebalanceInstructions(Map<String, Long> counts, Map<String, String> runModeProperties) {
            this.counts = counts;
            this.runModeProperties = runModeProperties;
        }

        // public getters are required by Jackson
        public Map<String, Long> getCounts() {
            return this.counts;
        }

        public void setCounts(Map<String, Long> counts) {
            this.counts = counts;
        }

        public Map<String, String> getRunModeProperties() { return this.runModeProperties; }

        public void setRunModeProperties(Map<String, String> runModeProperties) {
            this.runModeProperties = runModeProperties;
        }
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

    private Map<String, Future<HttpResponse>> requestRebalancing
            (Phase phase, Map<String, Long> executionsPerTest,
            ConcurrentHashMap<String, String> activeAgents,
            Map<String, String> recentlyAddedAgents,
            Configuration configuration) throws CloneNotSupportedException {
        Map<String, Future<HttpResponse>> newRunningTasks = new HashMap<>();
        // start by updating the number of tests that are left to be executed by the agents
        updateCountPerTest(phase, executionsPerTest);

        List<String> agentNames = new ArrayList<>(activeAgents.keySet());
        agentNames.addAll(recentlyAddedAgents.keySet());

        Map<String, Phase> phases = phasePartitioner.splitPhase(phase, agentNames);

        // System.out.println("[rebalancing]Size of agents : " + agentNames.size() + " : " + agentNames.toString());
        ObjectMapper mapper = new ObjectMapper();

        // convert each task test suite into instructions for agents to update their test suite
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
                System.out.println("[rebalancing] Sending " + instructionMessage + " to agent " + agentName);

                this.httpUtils.sendSyncHttpRequest(instructionMessage, agentURI);

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        });

        // for the recently added agents, send execution queries
        recentlyAddedAgents.forEach((newAgentName, newAgentIp) -> {
            configuration.setPhases(Collections.singletonList(phases.get(newAgentName)));
            YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
            String yamlTask = dumpConfig.generateConfigurationObject();
            String URI = "http://" + newAgentIp + ":4567" + "/submitTask";

            System.out.println("[task balancer] sending execution request to new agent " + newAgentName);
            Future<HttpResponse> future  =
                    this.httpUtils.sendAsyncHttpRequest(URI, yamlTask, this.asyncClient);
            newRunningTasks.put(newAgentName, future);

        });

        return newRunningTasks;
    }

    public Map<String, Future<HttpResponse>> rebalanceWork(Phase phase, Map<String, Long> executionsPerTest,
                              ConcurrentHashMap<String, String> activeAgents,
                              Map<String, String> recentlyAddedAgents,
                              Configuration configuration) {
        Map<String, Future<HttpResponse>> newRunningTasks = null;
        System.out.println("[Rebalance] Starting....");

        try {
           newRunningTasks = requestRebalancing(phase, executionsPerTest, activeAgents, recentlyAddedAgents, configuration);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        return newRunningTasks;
    }

}
