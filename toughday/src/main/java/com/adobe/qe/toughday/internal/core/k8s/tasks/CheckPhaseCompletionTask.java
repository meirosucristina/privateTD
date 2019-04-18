package com.adobe.qe.toughday.internal.core.k8s.tasks;

import org.apache.http.HttpResponse;

import java.util.Map;
import java.util.concurrent.Future;

public class CheckPhaseCompletionTask implements Runnable {
    private final Map<String, Future<HttpResponse>> runningTasks;

    public CheckPhaseCompletionTask(Map<String, Future<HttpResponse>> runningTasks) {
        this.runningTasks = runningTasks;
    }

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
