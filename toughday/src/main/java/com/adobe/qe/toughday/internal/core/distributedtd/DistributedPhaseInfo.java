package com.adobe.qe.toughday.internal.core.distributedtd;

import com.adobe.qe.toughday.internal.core.engine.Phase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class DistributedPhaseInfo {
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final List<String> agentsRunningTD = new ArrayList<>();
    // key = name of the test; value = map(key = name of the agent, value = nr of tests executed)
    private Map<String, Map<String, Long>> executions = new HashMap<>();
    private Phase phase;
    private long phaseStartTime = 0;

    public boolean isPhaseExecuting() {
        return this.phase != null && !this.agentsRunningTD.isEmpty();
    }

    public void setPhaseStartTime(long phaseStartTime) {
        this.phaseStartTime = phaseStartTime;
    }

    public long getPhaseStartTime() {
        return this.phaseStartTime;
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
        this.phase.getTestSuite().getTests().forEach(test -> executions.put(test.getName(), new HashMap<>()));
    }

    public void registerAgentRunningTD(String agentName) {
        this.agentsRunningTD.add(agentName);
    }

    public void removeAgentFromActiveTDRunners(String agentName) {
        this.agentsRunningTD.remove(agentName);
    }

    public void waitForPhaseCompletion() throws ExecutionException, InterruptedException {
        Future<?> waitPhaseCompletion = executorService.submit(() -> {
            while (!this.agentsRunningTD.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // this will not cause further problems => it can be ignored
                }
            }
        });

        waitPhaseCompletion.get();

    }

    public void updateCountPerTest() {
        phase.getTestSuite().getTests().forEach(test -> {
            long remained = test.getCount() - this.getExecutionsPerTest().get(test.getName());
            if (remained < 0) {
                // set this to 0 so that the agents will know to delete the test from the test suite
                test.setCount("0");
            } else {
                test.setCount(String.valueOf(remained));
            }
        });
    }

    public Map<String, Map<String, Long>> getExecutions() {
        return this.executions;
    }

    public void resetExecutions() {
        this.executions.forEach((testName, executionsPerAgent) ->
                executionsPerAgent.keySet().forEach(agentName -> executionsPerAgent.put(agentName, 0L)));
    }

    public Map<String, Long> getExecutionsPerTest() {
        Map<String, Long> executionsPerTest = new HashMap<>();

        this.executions.forEach((testName, executionsPerAgent) ->
                executionsPerTest.put(testName, executionsPerAgent.values().stream().mapToLong(x -> x).sum()));

        return executionsPerTest;
    }

    public void removeAgentFromExecutionsMap(String agentName) {
        this.executions.forEach((test, executionsPerAgent) -> this.executions.get(test).remove(agentName));
    }

    public Phase getPhase() {
        return this.phase;
    }

}