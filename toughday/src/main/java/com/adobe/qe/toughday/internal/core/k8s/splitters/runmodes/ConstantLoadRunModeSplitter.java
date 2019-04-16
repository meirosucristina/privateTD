package com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes;

import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.engine.runmodes.ConstantLoad;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConstantLoadRunModeSplitter implements RunMode.RunModeSplitter<ConstantLoad> {

    protected static final Logger LOG = LogManager.getLogger(ConstantLoadRunModeSplitter.class);

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
                                                                         List<String> newAgents, long phaseStartTime) {
        System.out.println("[constant load run mode splitter] Distributing for rebalancing work...");

        List<String> agents = new ArrayList<>(oldAgents);
        agents.addAll(newAgents);

        Map<String, ConstantLoad> taskRunModes = distributeRunMode(runMode, agents);

        if (runMode.isVariableLoad()) {
            // compute the current load to compute the new values for start/current load
            long endTime = System.currentTimeMillis();
            long diff = endTime - phaseStartTime;
            int estimatedCurrentLoad = ((int)(diff / GlobalArgs.parseDurationToSeconds(runMode.getInterval()))) / 1000
                    * runMode.getRate() + runMode.getStart();

            System.out.println("Phase was executed for " + (endTime - diff) / 1000 + " seconds");
            System.out.println("Estimated current load " + estimatedCurrentLoad);

            // set start property for new agents
            newAgents.forEach(agentName -> taskRunModes.get(agentName)
                    .setStart(String.valueOf(estimatedCurrentLoad / agents.size())));

            // set current load for old agents
            taskRunModes.get(oldAgents.get(0)).setCurrentLoad(estimatedCurrentLoad / agents.size() +
                     estimatedCurrentLoad % agents.size());
            for (int i = 1; i < oldAgents.size(); i++) {
                taskRunModes.get(oldAgents.get(i)).setCurrentLoad(estimatedCurrentLoad / agents.size());
            }

            return taskRunModes;
        }

        newAgents.forEach(agentName -> taskRunModes.get(agentName).setStart("0"));

        return taskRunModes;
    }

}
