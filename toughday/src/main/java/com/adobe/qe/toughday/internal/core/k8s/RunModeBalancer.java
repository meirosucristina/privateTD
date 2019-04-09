package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.engine.RunMode;

public interface RunModeBalancer<T extends RunMode> {
    void before(RebalanceInstructions rebalanceInstructions, T runMode);
    void processRunModeInstructions(RebalanceInstructions rebalanceInstructions, T runMode);
    void after(RebalanceInstructions rebalanceInstructions, T runMode);
}
