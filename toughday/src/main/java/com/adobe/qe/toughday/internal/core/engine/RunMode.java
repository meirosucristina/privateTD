/*
Copyright 2015 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/
package com.adobe.qe.toughday.internal.core.engine;

import com.adobe.qe.toughday.api.core.RunMap;
import com.adobe.qe.toughday.internal.core.k8s.RebalanceInstructions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public interface RunMode {
    void runTests(Engine engine) throws Exception;
    void finishExecutionAndAwait();
    ExecutorService getExecutorService();
    RunContext getRunContext();
    <T extends RunMode> RunModePartitioner<T> getRunModeSplitter();

    interface RunContext {
        Collection<AsyncTestWorker> getTestWorkers();
        Collection<RunMap> getRunMaps();
        boolean isRunFinished();
    }

    interface RunModePartitioner<T extends RunMode> extends Cloneable {
        Map<String, T> distributeRunMode(T runMode, List<String> agents);
        Map<String, T> distributeRunModeForRebalancingWork(T runMode, List<String> oldAgents, List<String> newAgents);
    }

     void processRebalanceInstructions(RebalanceInstructions rebalanceInstructions);
}
