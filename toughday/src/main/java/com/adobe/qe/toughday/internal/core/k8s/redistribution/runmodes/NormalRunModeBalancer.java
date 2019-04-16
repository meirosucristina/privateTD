package com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes;

import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.AsyncEngineWorker;
import com.adobe.qe.toughday.internal.core.engine.AsyncTestWorker;
import com.adobe.qe.toughday.internal.core.engine.runmodes.Normal;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NormalRunModeBalancer extends AbstractRunModeBalancer<Normal> {

    private void concurrencySanityChecks(Normal runMode, long newConcurrency) {
        // check that we have the exact number of active test workers
        if (runMode.getRunContext().getTestWorkers().size() != newConcurrency) {
            System.out.println("[rebalance processor - concurrency sanity checks] TestWorkers size is "
                    + runMode.getRunContext().getTestWorkers().size() + " but" + " new value for concurrency is " + newConcurrency);
            throw new IllegalStateException("[rebalance processor - concurrency sanity checks] TestWorkers size is "
                    + runMode.getRunContext().getTestWorkers().size() + " but" + " new value for concurrency is " + newConcurrency);
        }

        // check that all test workers are active
        if (runMode.getRunContext().getTestWorkers().stream().anyMatch(AsyncEngineWorker::isFinished)) {
            throw new IllegalStateException("[rebalance processor - concurrency sanity checks] " +
                    "There are finished test workers in the list of active workers.");
        }
    }

    private void reduceConcurrency(long reduce, Normal runMode) {
        List<AsyncTestWorker> workerList = new ArrayList<>(runMode.getRunContext().getTestWorkers());

        for (int i = 0; i < reduce; i++) {
            System.out.println("[rebalance processor] Finished test worker " + workerList.get(0).getWorkerThread().getId());
            runMode.finishAndDeleteWorker(workerList.get(i));
        }
    }

    private void increaseConcurrency(long increase, Normal runMode) {
        for (int i = 0; i < increase; i++) {
            System.out.println("[rebalance processor] Creating a new test worker...");
            runMode.createAndExecuteWorker(runMode.getEngine(), runMode.getEngine().getCurrentPhase().getTestSuite());
        }
    }

    private void processPropertyChange(String property, String newValue, Normal runMode) {
        if (property.equals("concurrency")) {

            System.out.println("[rebalance processor] Processing concurrency change");
            long currentConcurrency;
            if (runMode.isVariableConcurrency()) {
                currentConcurrency = runMode.getActiveThreads();
            } else {
                currentConcurrency = runMode.getConcurrency();
            }

            long newConcurrency = Long.parseLong(newValue);
            long difference = currentConcurrency - newConcurrency;

            System.out.println("[rebalance processor] currenct concurrency: " + currentConcurrency + "; new concurrency +" +
                    newConcurrency);

            System.out.println("[rebalance processsor] concurrency difference is " + difference);

            if (difference > 0) {
                // kill some test workers
                reduceConcurrency(difference, runMode);
            } else {
                // create a few more test workers
                increaseConcurrency(Math.abs(difference), runMode);
            }

            System.out.println("[rebalance processor] Test workers size: " + runMode.getRunContext().getTestWorkers().size());
            concurrencySanityChecks(runMode, newConcurrency);

            System.out.println("[rebalance processor] Successfully updated the state to respect the new value of concurrency.");
        }
    }


    @Override
    public void before(RebalanceInstructions rebalanceInstructions, Normal runMode) {
        System.out.println("[normal run mode balancer] - before...");
        if (runMode.isVariableConcurrency()) {
            /* We must cancel the scheduled task and reschedule it with the new values for 'period' and
             * initial delay.
             */
            boolean cancelled = runMode.getScheduledFuture().cancel(true);
            if (!cancelled) {
                System.out.println("[normal run mode balancer] task could not be cancelled.");
                return;
            }

            System.out.println("[normal run mode balancer] successfully cancelled task.");
        }

        Map<String, String> runModeProperties = rebalanceInstructions.getRunModeProperties();
        runModeProperties.forEach((propertyName, propValue) -> this.processPropertyChange(propertyName, propValue, runMode));
    }

    @Override
    public void after(RebalanceInstructions rebalanceInstructions, Normal runMode) {
        // TODO: should we wait for all the agents to confirm the interruption on the scheduled task?
        // reschedule the task
        if (runMode.isVariableConcurrency()) {
            if (runMode.getStart() < runMode.getEnd()) {
                Runnable rampUpRunnable = runMode.getRampUpRunnable(runMode.getEngine(),
                        runMode.getEngine().getCurrentPhase().getTestSuite());
                runMode.getAddWorkerScheduler().scheduleAtFixedRate(rampUpRunnable, runMode.getInitialDelay(),
                        GlobalArgs.parseDurationToSeconds(runMode.getInterval()) * 1000, TimeUnit.MILLISECONDS);

                System.out.println("[normal run mode balancer] successfully rescheduled ramp up with interval " +
                        runMode.getInterval() + " and initial delay " + runMode.getInitialDelay());

            } else if (runMode.getStart() > runMode.getEnd()) {
                Runnable rampDownRunnable = runMode.getRampDownRunnable();
                runMode.getRemoveWorkerScheduler().scheduleAtFixedRate(rampDownRunnable, runMode.getInitialDelay(),
                        GlobalArgs.parseDurationToSeconds(runMode.getInterval()) * 1000, TimeUnit.MILLISECONDS);

                System.out.println("[normal run mode balancer] successfully rescheduled ramp down with interval " +
                        runMode.getInterval() + " and initial delay " + runMode.getInitialDelay());
            }
        }

    }
}
