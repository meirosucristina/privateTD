package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.RunMode;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class responsible for dividing each phase into multiple tasks to be distributed between the
 * agents running in the K8s cluster.
 */
public class TaskPartitioner {

    public List<Phase> splitPhase(Phase phase, int nrAgents) throws CloneNotSupportedException {
        List<Phase> partitions = new ArrayList<>();
        List<RunMode> partitionRunModes = phase.getRunMode().distributeRunMode(nrAgents);

        for (int i = 0; i < 2; i++) {

            //Phase taskPhase = Phase.copy(Phase.class, phase);
            Phase taskPhase = (Phase) phase.clone();

            /* change count property for each test in the test suite */
            Map<AbstractTest, AtomicLong> newCounts = new HashMap<>();
            TestSuite taskTestSuite = phase.getTestSuite().clone();

            for (AbstractTest test : taskTestSuite.getTests()) {
               if (i % 2 == 0) {
                   newCounts.put(test, new AtomicLong(test.getCount() / nrAgents));
                   test.setCount(String.valueOf(newCounts.get(test)));
               } else {
                   newCounts.put(test, new AtomicLong(test.getCount() / nrAgents + test.getCount() % nrAgents));
                   test.setCount(String.valueOf(newCounts.get(test)));
               }
            }

            taskPhase.setCounts(newCounts);
            taskPhase.setTestSuite(taskTestSuite);

            /* set new run mode for current phase */
            taskPhase.setRunMode(partitionRunModes.get(i));

            /* add task to partition set */
            if (i % 2 == 0) {
                partitions.addAll(Collections.nCopies(nrAgents - 1, taskPhase));
            } else {
                partitions.add(taskPhase);
            }

        }

        return partitions;
    }
}
