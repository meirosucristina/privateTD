package com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes;

import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.engine.runmodes.Normal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NormalRunModeSplitter implements RunMode.RunModePartitioner<Normal> {

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
            /* we must distribute the concurrency level */
            clone.setConcurrency(String.valueOf(runMode.getConcurrency()/ nrAgents + concurrencyRemainder));
        }

        return clone;
    }


    @Override
    public Map<String, Normal> distributeRunMode(Normal runMode, List<String> agents) {
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
    public Map<String, Normal> distributeRunModeForRebalancingWork(Normal runMode, List<String> oldAgents, List<String> newAgents) {
        List<String> agents = new ArrayList<>(oldAgents);
        agents.addAll(newAgents);

        System.out.println("[Normal Run Mode Splitter] Splitter for rebalancing work was called.");

        Map<String, Normal> taskRunModes = distributeRunMode(runMode, agents);

        System.out.println("[Normal Run Mode Splitter] New agents is " + newAgents.toString());
        // set start property to 'rate' for each new agent
        if (runMode.isVariableConcurrency()) {
            newAgents.forEach(agentName -> {
                taskRunModes.get(agentName).setStart(String.valueOf(taskRunModes.get(agentName).getRate()));
                System.out.println("[Normal Run Mode Splitter] Setting start to " + taskRunModes.get(agentName).getRate() + " for agent " + agentName);
            });
        }

        return taskRunModes;
    }

}
