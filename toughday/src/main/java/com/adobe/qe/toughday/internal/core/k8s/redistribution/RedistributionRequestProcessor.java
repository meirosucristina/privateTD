package com.adobe.qe.toughday.internal.core.k8s.redistribution;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes.ConstantLoadRunModeBalancer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class RedistributionRequestProcessor {
    protected static final Logger LOG = LogManager.getLogger(ConstantLoadRunModeBalancer.class);

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
                    // remove tests for which the count property was achieved
                    if (counts.get(test.getName()) == 0) {
                        phase.getCounts().remove(test);
                    } else {
                        // reset number of tests executed so far
                        phase.getCounts().put(test, new AtomicLong(0));
                        // update number of executions left for this test
                        test.setCount(String.valueOf(counts.get(test.getName())));
                    }
                });
    }

    private void processRunModeChanges(RedistributionInstructions redistributionInstructions, RunMode runMode) {
        runMode.getRunModeBalancer().before(redistributionInstructions, runMode);

        runMode.getRunModeBalancer().processRunModeInstructions(redistributionInstructions, runMode);

        runMode.getRunModeBalancer().after(redistributionInstructions, runMode);
    }

    public void processRequest(Request request, Phase phase) throws IOException {
        String jsonContent = request.body();
        ObjectMapper objectMapper = new ObjectMapper();

        LOG.info("[Agent] Started processing rebalance instructions");
        RedistributionInstructions redistributionInstructions =
                objectMapper.readValue(jsonContent, RedistributionInstructions.class);

        // update values for each modified property
        processRunModeChanges(redistributionInstructions, phase.getRunMode());
        processTestSuiteChanges(redistributionInstructions.getCounts(), phase);
    }
}
