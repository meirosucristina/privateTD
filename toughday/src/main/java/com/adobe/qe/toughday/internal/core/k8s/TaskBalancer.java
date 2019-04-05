package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class responsible for balancing the work between the agents running the cluster
 * whenever the number of agents is changing.
 */
public class TaskBalancer {
    private static final String REBALANCE_TEST_SUITE_PATH = "/rebalanceTests";
    private static final String REBALANCE_RUN_MODE_PATH = "/rebalanceRunMode";
    private static final String REBALANCE_PATH = "/rebalance";

    private final TaskPartitioner taskPartitioner = new TaskPartitioner();

    /* Contains all the information needed by the agents for updating their configuration when
     * the work needs to be rebalanced.
     */
    private class RebalanceInstructions {
        private final Map<String, Long> counts;
        //private final Map<String, Object> runModeProperties;

        public RebalanceInstructions(Map<String, Long> counts) {
            this.counts = counts;
            //this.runModeProperties = runModeProperties;
        }

        // required by Jackson
        public Map<String, Long> getCounts() {
            return this.counts;
        }
    }

    private void updateCountPerTest(Phase phase, Map<String, Long> executionsPerTest) {
        phase.getTestSuite().getTests().forEach(test -> {
            long remained = test.getCount() - executionsPerTest.get(test.getName());
            if (remained < 0) {
                // set this to 0 so that the agents will know to delete the test from the test suite
                phase.getTestSuite().remove(test.getName());
            } else {
                test.setCount(String.valueOf(remained));
            }
        });
    }

    static void sendSyncHttpRequest(String requestContent, String agentURI) {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(agentURI);

        try {
            StringEntity params = new StringEntity(requestContent);
            request.setEntity(params);
            request.setHeader("Content-type", "text/plain");

            // submit request and wait for ack from agent
            HttpResponse response = httpClient.execute(request);
            System.out.println("Response code is " + response.getStatusLine().getStatusCode());

        } catch (IOException e)  {
            System.out.println("Http request could not be sent to  " + agentURI);
            System.out.println(e.getMessage());
        }
    }

    private void requestTestRebalancing(Phase phase, Map<String, Long> executionsPerTest,
                                        ConcurrentHashMap<String, String> activeAgents,
                                        Map<String, String> recentlyAddedAgents) {
        // start by updating the number of tests that are left to be executed by the agents
        updateCountPerTest(phase, executionsPerTest);
        List<String> agentNames = new ArrayList<>(activeAgents.keySet());
        agentNames.addAll(recentlyAddedAgents.keySet());

        System.out.println("[rebalancing]Size of agents : " + agentNames.size() + " : " + agentNames.toString());
        Map<String,TestSuite> taskTestSuites = taskPartitioner.distributeTestSuite(phase.getTestSuite(),
                agentNames);
        ObjectMapper mapper = new ObjectMapper();

        // convert each task test suite into instructions for agents to update their test suite
        activeAgents.forEach((agentName, agentIp) -> {
            String agentURI = "http://" + activeAgents.get(agentName) + ":4567" + REBALANCE_PATH;
            TestSuite testSuite = taskTestSuites.get(agentName);

            // send instructions to agent to change the count property for each test in test suite
            HashMap<String, Long> map = new HashMap<>();
            testSuite.getTests().forEach(test -> map.put(test.getName(), test.getCount()));

            RebalanceInstructions rebalanceInstructions = new RebalanceInstructions(map);
            try {
                String instructionMessage = mapper.writeValueAsString(rebalanceInstructions);
                System.out.println("[rebalancing] Sending " + instructionMessage + " to agent " + agentName);
                // TODO: THINK WHETHER THIS SHOULD BE ASYNC OR NOT
                sendSyncHttpRequest(instructionMessage, agentURI);

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        });

        // for the recently added agents, send execution queries
    }

    /*private void requestRunModeRebalancing(Phase phase, ConcurrentHashMap<String, String> activeAgents) {
        // build messages for updating run mode properties in the agent
        Map<String, String> runModeInstructions = phase.getRunMode()
                .getDriverRebalanceContext()
                .getInstructionsForRebalancingWork(phase, activeAgents.keySet().toArray(new String[0]));

        // send instructions to agents and wait for confirmation that run modes were updated
        runModeInstructions.forEach((agentName, instructions) -> {
            String agentURI = "http://" + activeAgents.get(agentName) + ":4567" + REBALANCE_RUN_MODE_PATH;
            sendSyncHttpRequest(instructions, agentURI);
        });
    }*/

    public void rebalanceWork(Phase phase, Map<String, Long> executionsPerTest,
                              ConcurrentHashMap<String, String> activeAgents,
                              Map<String, String> recentlyAddedAgents) {
        System.out.println("[Rebalance] Starting....");

        requestTestRebalancing(phase, executionsPerTest, activeAgents, recentlyAddedAgents);
        //requestRunModeRebalancing(phase, activeAgents);

        // mark recently added agents as active task executors
        activeAgents.putAll(recentlyAddedAgents);
        recentlyAddedAgents.clear();
    }

}
