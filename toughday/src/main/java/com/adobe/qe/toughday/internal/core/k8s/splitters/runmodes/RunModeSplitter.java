package com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes;

import com.adobe.qe.toughday.internal.core.engine.RunMode;

import java.util.List;
import java.util.Map;

public interface RunModeSplitter<T extends RunMode> {
    Map<String, T> distributeRunMode(T runMode, List<String> agents);
    Map<String, T> distributeRunModeForRebalancingWork(T runMode, List<String> oldAgents, List<String> newAgents,
                                                       long phaseStartTime);
}
