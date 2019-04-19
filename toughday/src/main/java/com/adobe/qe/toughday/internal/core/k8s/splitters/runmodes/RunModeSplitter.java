package com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes;

import com.adobe.qe.toughday.internal.core.engine.RunMode;

import java.util.List;
import java.util.Map;

/**
 * Common interface for all run mode splitters. Implement this interface to specify how a certain type of run mode
 * must be partitioned into multiple run modes to be distributed to the agents running in the cluster.
 * @param <T> Type of the run mode to be partitioned
 */
public interface RunModeSplitter<T extends RunMode> {
    Map<String, T> distributeRunMode(T runMode, List<String> agents);
    Map<String, T> distributeRunModeForRebalancingWork(T runMode, List<String> oldAgents, List<String> newAgents,
                                                       long phaseStartTime);
}
