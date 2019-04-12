package com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes;

import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.runmodes.ConstantLoad;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ConstantLoadRunModeBalancer extends AbstractRunModeBalancer<ConstantLoad> {

    private void processPropertyChange(String property, String newValue, ConstantLoad runMode) {
        if (property.equals("load") && !runMode.isVariableLoad()) {
            System.out.println("[rebalance processor] Processing load change");

            long newLoad = Long.parseLong(newValue);
            long difference = runMode.getLoad() - newLoad;

            if  (difference > 0 ) {
                // remove part of the local run maps used by the workers
                for (int i = 0; i < difference; i++) {
                    runMode.removeRunMap(0);
                }
            }
        }
    }

    @Override
    public void before(RebalanceInstructions rebalanceInstructions, ConstantLoad runMode) {
        System.out.println("[constant load run mode balancer] - before....");

        // TODO: remove tests from cache before changing the count property

        if (runMode.isVariableLoad()) {
            /* We must cancel the scheduled task and reschedule it with the new values for 'period' and
             * initial delay.
             */

            boolean cancelled = runMode.getScheduledFuture().cancel(true);
            if (!cancelled) {
                System.out.println("[constant load run mode balancer]  task could not be cancelled.");
                return;
            }

            System.out.println("[constant load run mode balancer] successfully cancelled task.");
        }

        Map<String, String> runModeProperties = rebalanceInstructions.getRunModeProperties();
        runModeProperties.forEach((property, propValue) ->
                processPropertyChange(property, propValue, runMode));
    }

    @Override
    public void after(RebalanceInstructions rebalanceInstructions, ConstantLoad runMode) {
        // reschedule the task
        if (runMode.isVariableLoad()) {
            Runnable rampingRunnable = runMode.getRampingRunnable();
            runMode.getRampingScheduler().scheduleAtFixedRate(rampingRunnable, runMode.getInitialDelay(),
                    GlobalArgs.parseDurationToSeconds(runMode.getInterval()), TimeUnit.MILLISECONDS);

            System.out.println("[constant load run mode balancer] successfully rescheduled ramping task " +
                    "with interval " + runMode.getInterval() + " and initial delay "
                    + runMode.getInitialDelay());
        }
    }
}
