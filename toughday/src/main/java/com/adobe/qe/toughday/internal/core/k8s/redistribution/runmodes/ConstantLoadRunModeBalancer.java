package com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes;

import com.adobe.qe.toughday.internal.core.engine.runmodes.ConstantLoad;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;

public class ConstantLoadRunModeBalancer extends AbstractRunModeBalancer<ConstantLoad> {
    @Override
    public void before(RebalanceInstructions rebalanceInstructions, ConstantLoad runMode) {
        System.out.println("[constant load run mode balancer] - before....");

        if (runMode.isVariableLoad()) {
            /* We must cancel the scheduled task and reschedule it with the new values for 'period' and
             * initial delay.
             */

            //boolean cancelled = runMode.
        }
    }

    @Override
    public void after(RebalanceInstructions rebalanceInstructions, ConstantLoad runMode) {

    }
}
