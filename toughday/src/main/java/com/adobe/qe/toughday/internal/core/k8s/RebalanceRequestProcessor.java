package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class RebalanceRequestProcessor {

    private void processTestSuiteChanges(Map<String, Long> counts, Phase phase) {
        TestSuite testSuite = phase.getTestSuite();
        long nr = testSuite.getTests().stream()
                .filter(test -> !counts.containsKey(test.getName()))
                .count();
        if (nr > 0) {
            throw new IllegalStateException("Instructions were not received for each test in the test suite.");
        }

        testSuite.getTests()
                .stream()
                .filter(test -> counts.containsKey(test.getName()))
                .forEach(test -> {
                    // reset number of tests executed so far
                    phase.getCounts().put(test, new AtomicLong(0));
                    // update number of executions left for this test
                    test.setCount(String.valueOf(counts.get(test.getName())));

                    System.out.println("[rebalance processor] Setting count for test " + test.getName() + " to value: " + counts.get(test.getName()));
                });

        System.out.println("[rebalance processor] new phase counts is " + phase.getCounts().toString());
    }

    private void processRunModeChanges(RebalanceInstructions rebalanceInstructions, RunMode runMode) {
        runMode.getRunModeBalancer().before(rebalanceInstructions, runMode);

        runMode.getRunModeBalancer().processRunModeInstructions(rebalanceInstructions, runMode);

        runMode.getRunModeBalancer().after(rebalanceInstructions, runMode);
    }

    public void processRequest(Request request, Phase phase) throws IOException {
        String jsonContent = request.body();
        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println("[rebalance processor] Starting...");
        RebalanceInstructions rebalanceInstructions =
                objectMapper.readValue(jsonContent, RebalanceInstructions.class);

        /*// prepare run mode for the new configuration
        phase.getRunMode().processRebalanceInstructions(rebalanceInstructions);
        System.out.println("[rebalance processor] Run mode has updated the agent. We are ready to modify run mode properties.");
        */

        // update values for each modified property
        processRunModeChanges(rebalanceInstructions, phase.getRunMode());
        System.out.println("[rebalance processor] Updated run mode properties.");
        processTestSuiteChanges(rebalanceInstructions.getCounts(), phase);
        System.out.println("[rebalance processor] Updated test suite properties.");
    }
}
