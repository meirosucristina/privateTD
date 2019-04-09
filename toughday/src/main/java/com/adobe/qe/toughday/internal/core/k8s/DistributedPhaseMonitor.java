package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.engine.Phase;
import org.apache.http.HttpResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DistributedPhaseMonitor {
    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final Map<String, Future<HttpResponse>> runningTasks = new HashMap<>();
    // key = name of the test; value = map(key = name of the agent, value = nr of tests executed)
    private Map<String, Map<String, Long>> executions = new HashMap<>();
    private Phase phase;

    public boolean isPhaseExecuting() {
        return this.phase != null && !this.runningTasks.isEmpty();
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
        Future future = executorService.submit(new StatusCheckerWorker());
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Map<String, Long>> getExecutions() {
        return this.executions;
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

    private class StatusCheckerWorker implements Runnable {
        @Override
        public synchronized void run() {
            long size = runningTasks.keySet().size();

            while (size > 0) {
                size = runningTasks.entrySet().stream()
                        .filter(entry -> !entry.getValue().isDone()).count();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // skip and continue
                }
            }

            // reset map of running tasks
            runningTasks.clear();
        }
    }

    public Phase getPhase() {
        return this.phase;
    }

}
