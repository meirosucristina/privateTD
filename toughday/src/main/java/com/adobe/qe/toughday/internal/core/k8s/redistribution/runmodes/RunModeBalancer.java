package com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes;

import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RedistributionInstructions;

import java.util.Map;

public interface RunModeBalancer<T extends RunMode> {
    Map<String, String> getRunModePropertiesToRedistribute(Class type, T runMode);
    void before(RedistributionInstructions redistributionInstructions, T runMode);
    void processRunModeInstructions(RedistributionInstructions redistributionInstructions, T runMode);
    void after(RedistributionInstructions redistributionInstructions, T runMode);
}
