package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.k8s.redistribution.RedistributionInstructions;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes.AbstractRunModeBalancer;

public class DummyRunModeBalancer extends AbstractRunModeBalancer<DummyRunMode> {
    @Override
    public void before(RedistributionInstructions redistributionInstructions, DummyRunMode runMode) {

    }

    @Override
    public void after(RedistributionInstructions redistributionInstructions, DummyRunMode runMode) {

    }
}
