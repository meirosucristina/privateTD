package com.adobe.qe.toughday.internal.core.k8s.splitters;

import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class responsible for dividing phases into multiple tasks to be distributed between the
 * agents running in the K8s cluster.
 */
public class PhaseSplitter {

    private Map<String, TestSuite> distributeTestSuite(TestSuite initialTestSuite, List<String> agents) {
        Map<String, TestSuite> taskTestSuites = new HashMap<>();

        for (int i = 0; i < agents.size(); i++) {
            TestSuite clone = initialTestSuite.clone();

            clone.getTests().forEach(test -> {
                if (taskTestSuites.isEmpty()) {
                    test.setCount(String.valueOf(test.getCount() / agents.size() + test.getCount() % agents.size()));
                } else {
                    test.setCount(String.valueOf(test.getCount() / agents.size()));
                }
            });

            taskTestSuites.put(agents.get(i), clone);
        }

        return taskTestSuites;
    }

    private void sanityChecks(Phase phase, List<String> agents) {
        if (phase == null || agents == null) {
            throw new IllegalArgumentException("Phase/List of agents must not be null");
        }

        if (agents.isEmpty()) {
            throw new IllegalStateException("At least one agent must be running in the cluster.");
        }

    }

    private Map<String, Phase> mapPhaseToAgent(Phase phase,Map<String, RunMode> partitionRunModes,
                                               Map<String, TestSuite> partitionTestSuites, List<String> agents)
            throws CloneNotSupportedException {
        Map<String, Phase> phasePerAgent = new HashMap<>();

        for (String agent : agents) {
            Phase taskPhase = (Phase) phase.clone();
            TestSuite taskTestSuite = partitionTestSuites.get(agent);

            // set the count (the number of executions since the beginning of the run) of each test to 0
            taskTestSuite.getTests().forEach(test -> taskPhase.getCounts().put(test, new AtomicLong(0)));

            taskPhase.setTestSuite(taskTestSuite);
            taskPhase.setRunMode(partitionRunModes.get(agent));

            phasePerAgent.put(agent, taskPhase);
        }

        return phasePerAgent;
    }

    /**
     * Knows how to divide a phase into a number of tasks equal to the number of agents running in the cluster.
     * @param phase the phase to be divided into tasks.
     * @param agents list with all the agents able to receive a task and to execute it.
     * @throws CloneNotSupportedException if the phase is not cloneable.
     */
    public Map<String, Phase> splitPhase(Phase phase, List<String> agents) throws CloneNotSupportedException {
        sanityChecks(phase, agents);

        Map<String, RunMode> partitionRunModes = phase.getRunMode().getRunModeSplitter().distributeRunMode(phase.getRunMode(), agents);
        Map<String, TestSuite> partitionTestSuites = distributeTestSuite(phase.getTestSuite(), agents);

        return mapPhaseToAgent(phase, partitionRunModes, partitionTestSuites, agents);
    }

    public Map<String, Phase> splitPhaseForRebalancingWork(Phase phase, List<String> oldAgents,
                                                           List<String> newAgents, long phaseStartTime)
            throws CloneNotSupportedException {
        sanityChecks(phase, oldAgents);

        List<String> allAgents = new ArrayList<>(oldAgents);
        allAgents.addAll(newAgents);

        Map<String, RunMode> partitionRunModes =
                phase.getRunMode().getRunModeSplitter().distributeRunModeForRebalancingWork(phase.getRunMode(), oldAgents,
                        newAgents, phaseStartTime);
        Map<String, TestSuite> partitionTestSuites = distributeTestSuite(phase.getTestSuite(), allAgents);

        return mapPhaseToAgent(phase, partitionRunModes, partitionTestSuites, allAgents);
    }
}