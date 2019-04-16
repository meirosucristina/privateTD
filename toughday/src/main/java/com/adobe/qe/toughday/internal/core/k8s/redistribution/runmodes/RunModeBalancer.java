package com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes;

import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;

import java.util.Map;

public interface RunModeBalancer<T extends RunMode> {
    Map<String, String> getRunModePropertiesToRedistribute(Class type, Object object);
    void before(RebalanceInstructions rebalanceInstructions, T runMode);
    void processRunModeInstructions(RebalanceInstructions rebalanceInstructions, T runMode);
    void after(RebalanceInstructions rebalanceInstructions, T runMode);
}
