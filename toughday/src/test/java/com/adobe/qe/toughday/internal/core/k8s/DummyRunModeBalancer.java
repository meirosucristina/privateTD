package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes.AbstractRunModeBalancer;

public class DummyRunModeBalancer extends AbstractRunModeBalancer<DummyRunMode> {
    @Override
    public void before(RebalanceInstructions rebalanceInstructions, DummyRunMode runMode) {

    }

    @Override
    public void after(RebalanceInstructions rebalanceInstructions, DummyRunMode runMode) {

    }
}
