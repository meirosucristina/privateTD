package com.adobe.qe.toughday.internal.core.distributedtd.splitters.runmodes;

import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.runmodes.Normal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to split the normal run mode into multiple normal run modes to be distributed to the agents
 * running in the cluster.
 */
public class NormalRunModeSplitter implements RunModeSplitter<Normal> {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private Normal setParamsForDistributedRunMode(Normal runMode, int nrAgents, int rateRemainder,
                                                  int endRemainder, int startRemainder,
                                                  int concurrencyRemainder, int agentId) {
        Normal clone;
        try {
            clone = (Normal) runMode.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            return runMode;
        }

        if (runMode.isVariableConcurrency()) {
            if (runMode.getRate() >= nrAgents) {
                clone.setRate(String.valueOf(runMode.getRate() / nrAgents + rateRemainder));
                clone.setStart(String.valueOf(runMode.getStart() / nrAgents + startRemainder));
                clone.setEnd(String.valueOf(runMode.getEnd() / nrAgents + endRemainder));
            } else {
                clone.setInitialDelay(agentId * GlobalArgs.parseDurationToSeconds(runMode.getInterval()) * 1000);
                long interval = GlobalArgs.parseDurationToSeconds(runMode.getInterval());
                clone.setInterval(String.valueOf(interval * nrAgents) + 's');

                if (agentId > 0) {
                    clone.setStart("0");
                    clone.setEnd(String.valueOf((runMode.getEnd() - runMode.getStart()) / nrAgents));
                } else {
                    long diff = runMode.getEnd() - runMode.getStart();
                    clone.setEnd(String.valueOf(diff / nrAgents + diff % nrAgents + runMode.getStart()));
                }
            }
        } else {
            clone.setConcurrency(String.valueOf(runMode.getConcurrency()/ nrAgents + concurrencyRemainder));
        }

        return clone;
    }


    @Override
    public Map<String, Normal> distributeRunMode(Normal runMode, List<String> agents) {
        if (runMode == null) {
            throw new IllegalArgumentException("Run mode must not be null.");
        }

        Map<String, Normal> taskRunModes = new HashMap<>();
        int nrAgents = agents.size();

        Normal firstTask = setParamsForDistributedRunMode(runMode, nrAgents, runMode.getRate() % nrAgents, runMode.getEnd() % nrAgents,
                runMode.getStart() % nrAgents, runMode.getConcurrency() % nrAgents, 0);
        taskRunModes.put(agents.get(0), firstTask);

        for (int i = 1; i < nrAgents; i++) {
            Normal task = setParamsForDistributedRunMode(runMode, nrAgents, 0, 0, 0, 0, i);
            taskRunModes.put(agents.get(i), task);
        }

        return taskRunModes;
    }

    @Override
    public Map<String, Normal> distributeRunModeForRebalancingWork(Normal runMode, List<String> oldAgents,
                                                                   List<String> newAgents, long phaseStartTime) {
        if (runMode == null) {
            throw new IllegalArgumentException("Run mode must not be null.");
        }

        List<String> agents = new ArrayList<>(oldAgents);
        agents.addAll(newAgents);
        Map<String, Normal> taskRunModes = distributeRunMode(runMode, agents);

        // set start property to 'rate' for each new agent
        if (!runMode.isVariableConcurrency()) {
            return taskRunModes;
        }

        // compute the expected current concurrency
        long endTime = System.currentTimeMillis();
        long diff = endTime - phaseStartTime;
        int estimatedConcurrency = ((int)(diff / GlobalArgs.parseDurationToSeconds(runMode.getInterval()))) / 1000
                * runMode.getRate() + runMode.getStart();

        LOG.info("Phase was executed for " + diff / 1000 + " seconds");
        LOG.info("Estimated concurrency " + estimatedConcurrency);

        // set start property for new agents
        newAgents.forEach(agentName ->
            taskRunModes.get(agentName).setStart(String.valueOf(estimatedConcurrency / agents.size())));

        // set desired active threads for old agents
        taskRunModes.get(oldAgents.get(0)).setConcurrency(String.valueOf(estimatedConcurrency / agents.size() +
                estimatedConcurrency % agents.size()));
        for (String oldAgent : oldAgents) {
            taskRunModes.get(oldAgent).setConcurrency(String.valueOf(estimatedConcurrency / agents.size()));
        }

        return taskRunModes;

    }

}
