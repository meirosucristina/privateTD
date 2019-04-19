package com.adobe.qe.toughday.internal.core.k8s.tasks;

import com.adobe.qe.toughday.internal.core.k8s.DistributedPhaseInfo;
import org.apache.http.HttpResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

public class CheckAgentRunsToughday implements Runnable {
    private final List<ScheduledFuture<Map<String, Future<HttpResponse>>>> newRunningTasks;
    private final DistributedPhaseInfo distributedPhaseInfo;

    public CheckAgentRunsToughday(List<ScheduledFuture<Map<String, Future<HttpResponse>>>> newRunningTasks,
                                  DistributedPhaseInfo distributedPhaseInfo) {
        this.newRunningTasks = newRunningTasks;
        this.distributedPhaseInfo = distributedPhaseInfo;
    }

    @Override
    public void run() {
        /* we should periodically check if recently added agents started executing tasks and
         * we should monitor them.
         */
        List<ScheduledFuture<Map<String, Future<HttpResponse>>>> finishedFutures = this.newRunningTasks.stream()
                .filter(Future::isDone)
                .collect(Collectors.toList());

        finishedFutures.forEach(future -> {
            try {
                future.get().forEach(this.distributedPhaseInfo::registerRunningTask);
                this.newRunningTasks.removeAll(finishedFutures);
            } catch (InterruptedException | ExecutionException e) {
                // this code should never be reached since futures are in Done state.
                e.printStackTrace();
            }
        });
    }
}
