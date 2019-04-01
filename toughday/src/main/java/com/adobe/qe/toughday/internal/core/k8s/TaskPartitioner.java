package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class responsible for dividing phases into multiple tasks to be distributed between the
 * agents running in the K8s cluster.
 */
public class TaskPartitioner {

    private Phase setParamsForDistributedPhase(Phase phase, int nrAgents, RunMode runMode, boolean addRemainder)
            throws CloneNotSupportedException {
        Phase taskPhase = (Phase) phase.clone();

        /* change count property for each test in the test suite */
        TestSuite taskTestSuite = phase.getTestSuite().clone();

        // set the count (the number of executions since the beginning of the run) of each test to 0
        taskTestSuite.getTests().forEach(test -> taskPhase.getCounts().put(test, new AtomicLong(0)));

        taskTestSuite.getTests().forEach(test -> {
            if (addRemainder) {
                test.setCount(String.valueOf(test.getCount() / nrAgents + test.getCount() % nrAgents));
            } else {
                test.setCount(String.valueOf(test.getCount() / nrAgents));
            }
        });

        taskPhase.setTestSuite(taskTestSuite);

        /* set new run mode for current phase */
        taskPhase.setRunMode(runMode);

        return taskPhase;
    }

    /**
     * Knows how to divide a phase into a number of tasks equal to the number of agents running in the cluster.
     * @param phase the phase to be partitioned into tasks.
     * @param agents list with all the agents able to receive a task and to execute it.
     * @throws CloneNotSupportedException if the phase is not cloneable.
     */
    public Map<String, Phase> splitPhase(Phase phase, List<String> agents) throws CloneNotSupportedException {
        if (phase == null || agents == null) {
            throw new IllegalArgumentException("Phase/List of agents must not be null");
        }

        if (agents.isEmpty()) {
            throw new IllegalStateException("At least one agent must be running in the cluster.");
        }

        Map<String, Phase> taskPerAgent = new HashMap<>();
        List<RunMode> partitionRunModes = phase.getRunMode().distributeRunMode(agents.size());

        for (int i = 0; i < agents.size(); i++) {
            taskPerAgent.put(agents.get(i), setParamsForDistributedPhase(phase, agents.size(),
                    partitionRunModes.get(i), i == 0));
        }

        return taskPerAgent;
    }
}
