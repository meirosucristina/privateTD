package com.adobe.qe.toughday.internal.core.distributedtd;

import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.CheckPhaseCompletionTask;
import org.apache.http.HttpResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DistributedPhaseInfo {
    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final Map<String, Future<HttpResponse>> runningTasks = new HashMap<>();
    // key = name of the test; value = map(key = name of the agent, value = nr of tests executed)
    private Map<String, Map<String, Long>> executions = new HashMap<>();
    private Phase phase;
    private long phaseStartTime = 0;

    public boolean isPhaseExecuting() {
        return this.phase != null && !this.runningTasks.isEmpty();
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

    public void registerRunningTask(String agentName, Future<HttpResponse> agentResponse) {
        this.runningTasks.put(agentName, agentResponse);
    }

    public void removeRunningTask(String agentName) {
        this.runningTasks.remove(agentName);
    }

    public void waitForPhaseCompletion() {
        Future future = executorService.submit(new CheckPhaseCompletionTask(this.runningTasks));
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        this.runningTasks.clear();
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