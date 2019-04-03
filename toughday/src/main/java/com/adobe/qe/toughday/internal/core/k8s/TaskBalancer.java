package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class responsible for balancing the work between the agents running the cluster
 * whenever the number of agents is changing.
 */
public class TaskBalancer {
    private static final String REBALANCE_TEST_SUITE_PATH = "/rebalanceTests";
    private static final String REBALANCE_RUN_MODE_PATH = "/rebalanceRunMode";

    private final TaskPartitioner taskPartitioner = new TaskPartitioner();

    private void updateCountPerTest(Phase phase, Map<String, Long> executionsPerTest) {
        phase.getTestSuite().getTests().forEach(test -> {
            long remained = test.getCount() - executionsPerTest.get(test);
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

    private void requestTestRebalancing(Phase phase, Map<String, Long> executionsPerTest, ConcurrentHashMap<String, String> activeAgents) {
        Gson gson = new Gson();

        // start by updating the number of tests that are left to be executed by the agents
        updateCountPerTest(phase, executionsPerTest);

        Map<String,TestSuite> taskTestSuites = taskPartitioner.distributeTestSuite(phase.getTestSuite(),
                new ArrayList<>(activeAgents.keySet()));

        // convert each task test suite into instructions for agents to update their test suite
        taskTestSuites.forEach((agentName, testSuite) -> {
            String agentURI = "http://" + activeAgents.get(agentName) + ":4567" + REBALANCE_TEST_SUITE_PATH;

            // send instructions to agent to change the count property for each test in test suite
            HashMap<String, Long> map = new HashMap<>();
            testSuite.getTests().forEach(test -> map.put(test.getName(), test.getCount()));
            sendSyncHttpRequest(gson.toJson(map), agentURI);
        });
    }

    private void requestRunModeRebalancing(Phase phase, ConcurrentHashMap<String, String> activeAgents) {
        // build messages for updating run mode properties in the agent
        Map<String, String> runModeInstructions = phase.getRunMode()
                .getDriverRebalanceContext()
                .getInstructionsForRebalancingWork(phase, activeAgents.keySet().toArray(new String[0]));

        // send instructions to agents and wait for confirmation that run modes were updated
        runModeInstructions.forEach((agentName, instructions) -> {
            String agentURI = "http://" + activeAgents.get(agentName) + ":4567" + REBALANCE_RUN_MODE_PATH;
            sendSyncHttpRequest(instructions, agentURI);
        });
    }

    public void rebalanceWork(Phase phase, Map<String, Long> executionsPerTest, ConcurrentHashMap<String, String> activeAgents) {
        requestTestRebalancing(phase, executionsPerTest, activeAgents);
        requestRunModeRebalancing(phase, activeAgents);
    }

}
