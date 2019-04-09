package com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes;

import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.AsyncTestWorker;
import com.adobe.qe.toughday.internal.core.engine.runmodes.Normal;
import com.adobe.qe.toughday.internal.core.k8s.AbstractRunModeBalancer;
import com.adobe.qe.toughday.internal.core.k8s.RebalanceInstructions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NormalRunModeBalancer extends AbstractRunModeBalancer<Normal> {

    private void processPropertyChange(String property, String newValue, Normal runMode) {
        if (property.equals("concurrency") && !runMode.isVariableConcurrency()) {
            System.out.println("[rebalance processor] Processing concurrency change");
            long newConcurrency = Long.parseLong(newValue);
            long difference = runMode.getConcurrency() - newConcurrency;
            List<AsyncTestWorker> workerList = new ArrayList<>(runMode.getRunContext().getTestWorkers());

            System.out.println("[rebalance processsor] concurrency difference is " + difference);

            if (difference > 0) {
                // kill some test workers
                for (int i = 0; i < difference; i++) {
                    workerList.get(i).finishExecution();
                    workerList.remove(i);
                    System.out.println("[rebalance processor] Finished test worker " + workerList.get(i).getWorkerThread().getId());
                }
            } else {
                // create a few more test workers
                for (int i = 0; i < Math.abs(difference); i++) {
                    System.out.println("[rebalance processor] Creating a new test worker...");
                    runMode.createAndExecuteWorker(runMode.getEngine(), runMode.getEngine().getCurrentPhase().getTestSuite());
                }
            }

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
