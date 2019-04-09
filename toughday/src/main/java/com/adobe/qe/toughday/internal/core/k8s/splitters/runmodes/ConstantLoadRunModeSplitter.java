package com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes;

import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.engine.runmodes.ConstantLoad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConstantLoadRunModeSplitter implements RunMode.RunModeSplitter<ConstantLoad> {

    private ConstantLoad setParamsForDistributedRunMode(ConstantLoad runMode, int nrAgents, int rateRemainder,
                                                        int startRemainder, int endRemainder,
                                                        int loadRemainder, int agentId) {
        ConstantLoad clone;
        try {
            clone = (ConstantLoad) runMode.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            return runMode;
        }

        if (runMode.isVariableLoad()) {
            if (runMode.getRate() >= nrAgents) {
                clone.setStart(String.valueOf(runMode.getStart()/ nrAgents + startRemainder));
                clone.setEnd(String.valueOf(runMode.getEnd()/ nrAgents + endRemainder));
                clone.setRate(String.valueOf(runMode.getRate()/ nrAgents + rateRemainder));
            } else {
                clone.setInitialDelay(agentId * GlobalArgs.parseDurationToSeconds(runMode.getInterval()) * 1000);
                clone.setOneAgentRate(runMode.getRate());
                clone.setStart(String.valueOf(runMode.getStart()+ agentId * runMode.getRate()));
                clone.setRate(String.valueOf(nrAgents * runMode.getRate()));
                long interval = GlobalArgs.parseDurationToSeconds(runMode.getInterval());
                clone.setInterval(String.valueOf(interval * nrAgents) + 's');
            }

            return clone;
        }

        /* we must distribute the load between the agents */
        clone.setLoad(String.valueOf(runMode.getLoad() / nrAgents + loadRemainder));

        return clone;
    }

    @Override
    public Map<String, ConstantLoad> distributeRunMode(ConstantLoad runMode, List<String> agents) {
        int nrAgents = agents.size();
        Map<String, ConstantLoad> runModes = new HashMap<>();

        ConstantLoad firstTask = setParamsForDistributedRunMode(runMode, nrAgents, runMode.getRate() % nrAgents,
                runMode.getStart() % nrAgents, runMode.getEnd() % nrAgents, runMode.getLoad() % nrAgents, 0);
        runModes.put(agents.get(0), firstTask);

        for (int i = 1; i < nrAgents; i++) {
            ConstantLoad task = setParamsForDistributedRunMode(runMode, nrAgents, 0, 0, 0, 0, i);
            runModes.put(agents.get(i), task);
        }

        return runModes;
    }

    @Override
    public Map<String, ConstantLoad> distributeRunModeForRebalancingWork(ConstantLoad runMode, List<String> oldAgents,
                                                                         List<String> newAgents) {
        List<String> agents = new ArrayList<>(oldAgents);
        agents.addAll(newAgents);

        Map<String, ConstantLoad> taskRunModes = distributeRunMode(runMode, agents);

        // set start property to 'rate' for each new agent
        newAgents.forEach(agentName -> taskRunModes.get(agentName).setStart(String.valueOf(taskRunModes.get(agentName).getRate())));

        return taskRunModes;
    }

}
