package com.adobe.qe.toughday.internal.core.engine.k8s;

import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.internal.core.config.ConfigParams;
import com.adobe.qe.toughday.internal.core.engine.Phase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class responsible for dividing each phase into multiple tasks to be distributed between the
 * agents running in the cluster.
 */
public class TaskPartitioner {

    public List<Phase> splitPhase(Phase phase, int nrAgents) {
        List<Phase> partitions = new ArrayList<>();

        for (int i = 0; i < nrAgents - 1; i++) {
            Phase taskPhase = ConfigParams.deepClone(phase);

            /* change count property for each test in the test suite */
            Map<AbstractTest, AtomicLong> newCounts = new HashMap<>();
            taskPhase.getTestSuite().getTests().forEach(
                    test -> {
                        newCounts.put(test, new AtomicLong(test.getCount() / nrAgents));
                        test.setCount(String.valueOf(test.getCount() / nrAgents));
            });
            taskPhase.setCounts(newCounts);

            /* set duration of task */
            taskPhase.setDuration(String.valueOf(phase.getDuration() / nrAgents));

            partitions.add(taskPhase);
        }

        /* create last task */
        Phase lastTaskPhase = ConfigParams.deepClone(phase);
        Map<AbstractTest, AtomicLong> newCounts = new HashMap<>();
        lastTaskPhase.getTestSuite().getTests().forEach(
                test -> {
                    newCounts.put(test, new AtomicLong(test.getCount() / nrAgents + test.getCount() % nrAgents));
                    test.setCount(String.valueOf(newCounts.get(test)));
                });
        lastTaskPhase.setDuration(String.valueOf(phase.getDuration() / nrAgents + phase.getDuration() % nrAgents));
        lastTaskPhase.setCounts(newCounts);

        /* add task to partition set */
        partitions.add(lastTaskPhase);


        return partitions;
    }


}
