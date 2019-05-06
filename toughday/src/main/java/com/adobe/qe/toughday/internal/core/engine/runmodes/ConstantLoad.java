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
package com.adobe.qe.toughday.internal.core.engine.runmodes;

import com.adobe.qe.toughday.api.annotations.labels.NotNull;
import com.adobe.qe.toughday.api.annotations.labels.Nullable;
import com.adobe.qe.toughday.api.core.*;
import com.adobe.qe.toughday.api.annotations.Description;
import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.internal.core.TestSuite;
import com.adobe.qe.toughday.internal.core.engine.*;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.runmodes.ConstantLoadRunModeBalancer;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.runmodes.RunModeBalancer;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.runmodes.ConstantLoadRunModeSplitter;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.runmodes.RunModeSplitter;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Description(desc = "Generates a constant load of test executions, regardless of their execution time.")
public class ConstantLoad implements RunMode, Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(ConstantLoad.class);

    private static final String DEFAULT_LOAD_STRING = "50";
    private static final int DEFAULT_LOAD = Integer.parseInt(DEFAULT_LOAD_STRING);
    private static final String DEFAULT_INTERVAL_STRING = "1s";
    private static final long DEFAULT_INTERVAL = 1000;

    private AtomicBoolean loggedWarning = new AtomicBoolean(false);

    private ExecutorService executorService = Executors.newCachedThreadPool();
    private Collection<AsyncTestWorker> testWorkers = Collections.synchronizedSet(new HashSet<AsyncTestWorker>());
    private AsyncTestWorkerScheduler scheduler;
    private final List<RunMap> runMaps = new ArrayList<>();
    private int load = DEFAULT_LOAD;
    private int start = DEFAULT_LOAD;
    private int end = DEFAULT_LOAD;
    private long interval = DEFAULT_INTERVAL;
    private int rate;
    private int currentLoad;
<<<<<<< HEAD
    //private int currentLoad;
    private long initialDelay = 0;
    /* field used for checking the finish condition when running TD distributed with the rate
     * smaller than number of agents in the cluster. */
=======
    private long initialDelay = 0;
    /* field used for checking the finish condition when running TD distributed on K8S with
    the rate smaller than number of agents in the cluster. */
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
    private int oneAgentRate = 0;

    private ScheduledExecutorService runRoundScheduler = Executors.newScheduledThreadPool(1);
    private TestCache testCache;
    private Phase phase;
    private RunModeSplitter<ConstantLoad> runModeSplitter = new ConstantLoadRunModeSplitter();
    private ScheduledFuture<?> scheduledFuture = null;
    private final ConstantLoadRunModeBalancer runModeBalancer = new ConstantLoadRunModeBalancer();

    private Boolean measurable = true;

    @ConfigArgSet(required = false, defaultValue = DEFAULT_LOAD_STRING,
            desc = "Set the load, in requests per second for the \"constantload\" runmodes.")
    public void setLoad(String load) {
        checkNotNegative(Long.parseLong(load), "load");
        this.load = Integer.parseInt(load);
    }

    @ConfigArgGet(redistribute = true)
    public int getLoad() { return this.load; }

    @ConfigArgGet(redistribute = true)
    public int getStart() {
        return start;
    }

    @ConfigArgSet(required = false, desc = "The load to start ramping up from. Will rise to the number specified by \"concurrency\".",
            defaultValue = "-1")
    public void setStart(String start) {
        if (!start.equals("-1")) {
            checkNotNegative(Long.parseLong(start), "start");
        }
        this.start = Integer.valueOf(start);
    }

    @ConfigArgGet(redistribute = true)
    public int getRate() {
        return rate;
    }

    @ConfigArgSet(required = false, desc = "The increase in load per time unit. When it equals -1, it means it is not set.",
            defaultValue = "-1")
    public void setRate(String rate) {
        if (!rate.equals("-1")) {
            checkNotNegative(Long.parseLong(rate), "rate");
        }
        this.rate = Integer.valueOf(rate);
    }

<<<<<<< HEAD
    @ConfigArgGet(redistribute = true)
=======
    @ConfigArgGet
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
    public String getInterval() {
        return String.valueOf(interval / 1000) + 's';
    }

    @ConfigArgSet(required = false, desc = "Used with rate to specify the time interval to add increase the load.",
            defaultValue = DEFAULT_INTERVAL_STRING)
    public void setInterval(String interval) {
        this.interval = GlobalArgs.parseDurationToSeconds(interval) * 1000;
    }

    @ConfigArgGet(redistribute = true)
    public int getEnd() {
        return end;
    }

    @ConfigArgSet(required = false, desc = "The maximum value that load reaches.", defaultValue = "-1")
    public void setEnd(String end) {
        if (!end.equals("-1")) {
            checkNotNegative(Long.parseLong(end), "end");
        }
        this.end = Integer.valueOf(end);
    }

<<<<<<< HEAD
    public ConstantLoad() {
        /* this is required when running TD distributed because scheduled task might be cancelled and
         * rescheduled when rebalancing the work between the agents.
         */
        ScheduledThreadPoolExecutor scheduledPoolExecutor = (ScheduledThreadPoolExecutor) runRoundScheduler;
        scheduledPoolExecutor.setRemoveOnCancelPolicy(true);
    }

=======
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
    public int getOneAgentRate() {
        return this.oneAgentRate;
    }

<<<<<<< HEAD
    public void setCurrentLoad(int currentLoad) {
        this.currentLoad = currentLoad;
    }

    public int getCurrentLoad() {
        return this.currentLoad;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

=======
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
    private static class TestCache {
        public Map<TestId, Queue<AbstractTest>> cache = new HashMap<>();

        public TestCache(TestSuite testSuite) {
            for(AbstractTest test : testSuite.getTests()) {
                cache.put(test.getId(), new ConcurrentLinkedQueue());
            }
        }

        public void add(@NotNull AbstractTest test) {
            cache.get(test.getId()).add(test);
        }

        public @Nullable AbstractTest getCachedValue(@NotNull TestId testID) {
            return cache.get(testID).poll();
        }
    }

    public boolean isVariableLoad() {
        return start != -1 && end != -1;
    }

    private void checkNotNegative(long param, String property) {
        if (param < 0) {
            throw new IllegalArgumentException("Property " + property + " incorrectly configured as negative.");
        }
    }

    private void checkInvalidArgs() {
        if ((start != -1 && end == -1) || (start == -1 && end != -1)) {
            throw new IllegalArgumentException("Cannot configure only one limit (start/end) for Constant Load mode.");
        }

        if (isVariableLoad() && load != DEFAULT_LOAD) {
            throw new IllegalArgumentException("Constant Load mode cannot be configured with both start/end and load.");
        }
    }

    public void addRunMaps(long nr) {
        for (long i = 0; i < nr; i++) {
            synchronized (runMaps) {
                runMaps.add(phase.getPublishMode().getRunMap().newInstance());
            }
        }
    }

    public void removeRunMaps(long nr) {
        for (long i = 0; i < nr; i++) {
            synchronized (runMaps) {
                this.runMaps.remove(0);
            }
        }
    }

    @Override
    public void runTests(Engine engine) {
        checkInvalidArgs();

        this.phase = engine.getCurrentPhase();
        TestSuite testSuite = phase.getTestSuite();

        this.testCache = new TestCache(testSuite);
        this.measurable = phase.getMeasurable();

        if (isVariableLoad()) {
            load = Math.max(start, end);
        }

        addRunMaps(load);

        this.scheduler = new AsyncTestWorkerScheduler(engine);
        executorService.execute(scheduler);
    }

    public RunContext getRunContext() {
        return new RunContext() {
            @Override
            public Collection<AsyncTestWorker> getTestWorkers() {
                return testWorkers;
            }

            @Override
            public Collection<RunMap> getRunMaps() {
                return runMaps;
            }

            @Override
            public boolean isRunFinished() {
                return scheduler != null && scheduler.isFinished();
            }
        };
    }

    private ConstantLoad setParamsForDistributedRunMode(int nrAgents, int rateRemainder,
                                                        int startRemainder, int endRemainder,
                                                        int loadRemainder, int agentId) {
        ConstantLoad clone = null;
        try {
            clone = (ConstantLoad) this.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        if (isVariableLoad()) {
            if (this.rate > nrAgents) {
                clone.setStart(String.valueOf(this.getStart() / nrAgents + startRemainder));
                clone.setEnd(String.valueOf(this.getEnd() / nrAgents + endRemainder));
                clone.setRate(String.valueOf(this.getRate() / nrAgents + rateRemainder));
            } else {
                clone.initialDelay = agentId * this.interval;
                clone.oneAgentRate = this.rate;
                clone.setStart(String.valueOf(this.start + agentId * this.rate));
                clone.setRate(String.valueOf(nrAgents * this.rate));
                clone.setInterval(String.valueOf(this.interval / 1000 * nrAgents) + 's');
            }

            return clone;
        }

        /* we must distribute the load between the agents */
        clone.setLoad(String.valueOf(this.getLoad() / nrAgents + loadRemainder));

        return clone;
    }

    @Override
    public List<RunMode> distributeRunMode(int nrAgents) {
        List<RunMode> runModes = new ArrayList<>();

        ConstantLoad firstTask = setParamsForDistributedRunMode(nrAgents, this.rate % nrAgents,
                this.start % nrAgents, this.end % nrAgents, this.load % nrAgents, 0);
        runModes.add(firstTask);

        for (int i = 1; i < nrAgents; i++) {
            ConstantLoad task = setParamsForDistributedRunMode(nrAgents, 0, 0, 0, 0, i);
            runModes.add(task);
        }

        return runModes;
    }

    @Override
    public RunModeBalancer<ConstantLoad> getRunModeBalancer() {
        return this.runModeBalancer;
    }

    public void setInitialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
    }

    public void setOneAgentRate(int oneAgentRate) {
        this.oneAgentRate = oneAgentRate;
    }

    @Override
    public RunModeSplitter<ConstantLoad> getRunModeSplitter() {
        return this.runModeSplitter;
    }

    @Override
    public void finishExecutionAndAwait() {
        scheduler.finishExecution();

        synchronized (testWorkers) {
            for(AsyncTestWorker testWorker : testWorkers) {
                testWorker.finishExecution();
            }
        }

        boolean allExited = false;
        while(!allExited) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
            }
            allExited = true;

            synchronized (testWorkers) {
                for (AsyncTestWorker testWorker : testWorkers) {
                    if (!testWorker.hasExited()) {
                        if(!testWorker.getMutex().tryLock())
                            continue;
                        allExited = false;
                        testWorker.getWorkerThread().interrupt();
                        testWorker.getMutex().unlock();
                    }
                }
            }
        }

    }

    private Runnable getRunnableToSchedule() {
        return this.scheduler.getRunnableToSchedule(new MutableLong((initialDelay + interval) / 1000));
    }

    private class AsyncTestWorkerImpl extends AsyncTestWorker {
        private AbstractTest test;
        private RunMap runMap;
        private boolean exited = false;

        public AsyncTestWorkerImpl(AbstractTest test, RunMap runMap) {
            this.test = test;
            this.runMap = runMap;
        }

        @Override
        public void run() {
            mutex.lock();
            lastTestStart = System.nanoTime();
            workerThread = Thread.currentThread();
            currentTest = test;
            mutex.unlock();
            try {
                AbstractTestRunner runner = RunnersContainer.getInstance().getRunner(test);
                runner.runTest(test, runMap);
            } catch (Throwable e) {
                LOG.warn("Exceptions from tests should not reach this point", e);
            }

            mutex.lock();
            currentTest = null;
            exited = true;
            testCache.add(test);
            Thread.interrupted();
            mutex.unlock();
        }

        @Override
        public boolean hasExited() {
            return exited;
        }
    }

    public boolean cancelPeriodicTask() {
        return this.scheduledFuture.cancel(true);
    }

    public void schedulePeriodicTask() {
        this.runRoundScheduler.scheduleAtFixedRate(getRunnableToSchedule(), 0,
                GlobalArgs.parseDurationToSeconds("1s"), TimeUnit.SECONDS);
    }


    private class AsyncTestWorkerScheduler extends AsyncEngineWorker {
        private Engine engine;

        public AsyncTestWorkerScheduler(Engine engine) {
            this.engine = engine;
        }

        private void configureRateAndInterval() {
            //the difference from the beginning load to the end one
            int loadDifference = Math.abs(end - start);

            // suppose load will increase by second
<<<<<<< HEAD
            long newInterval = 1;
            rate = (int)Math.floor(1.0 * newInterval* loadDifference
=======
            secondsLeft.setValue(1);
            rate = (int)Math.floor(1.0 * secondsLeft.getValue() * loadDifference
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
                    / GlobalArgs.parseDurationToSeconds(phase.getDuration()));

            // if the rate becomes too small, increase the interval at which the load is increased
            while (rate < 1) {
<<<<<<< HEAD
                newInterval += 1;
                rate = (int)Math.floor(1.0 * newInterval * loadDifference
=======
                secondsLeft.increment();
                rate = (int)Math.floor(1.0 * secondsLeft.getValue() * loadDifference
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
                        / GlobalArgs.parseDurationToSeconds(phase.getDuration()));
            }

            interval = newInterval * 1000; // set interval in milliseconds
        }

<<<<<<< HEAD
        public Runnable getRunnableToSchedule(MutableLong secondsLeft) {
            return () -> {
                if (!isFinished()) {
                    try {
                        // run the current run with the current load
                        runRound();
                        secondsLeft.decrement();
=======
        private void initialDelay() {
            long timeToSleep = initialDelay;

            while (timeToSleep > 0) {
                long start = System.currentTimeMillis();
                try {
                    Thread.sleep(timeToSleep);
                    break;
                } catch (InterruptedException e) {
                    timeToSleep = timeToSleep - (System.currentTimeMillis() - start);
                    LOG.warn("Thread was interrupted during the initial delay;");
                }
            }
        }

        @Override
        public void run() {
            if (initialDelay != 0)
                initialDelay();

            try {
                currentLoad = load;
                MutableLong secondsLeft = new MutableLong(interval);
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc

                        rampUp(secondsLeft);

                        rampDown(secondsLeft);

                    } catch (InterruptedException e) {
                        finishExecution();
                        LOG.warn("Constant load scheduler thread was interrupted.");

                        // gracefully shut down scheduler
                        runRoundScheduler.shutdownNow();
                    }
                }
            };
        }

        @Override
        public void run() {
            currentLoad = load;

            // if the rate was not specified and start and end were
            if (isVariableLoad()) {
                if (rate == -1) {
                    configureRateAndInterval();
                }

                currentLoad = start;
            }


            MutableLong secondsLeft = new MutableLong((interval +  initialDelay) / 1000);
            LOG.info("Seconds left is " + secondsLeft.getValue() + "s");

            scheduledFuture = runRoundScheduler.scheduleAtFixedRate(getRunnableToSchedule(secondsLeft),
                    0, GlobalArgs.parseDurationToSeconds("1s"), TimeUnit.SECONDS);

        }

        private void rampUp(MutableLong secondsLeft) {
            if (currentLoad == end || ((oneAgentRate > 0) && (currentLoad + rate >= end + oneAgentRate))) {
<<<<<<< HEAD
                LOG.info("[ramp up] Execution finished...");
=======
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
                finishExecution();
            }

            // if 'interval' has passed and the current load is still below 'end',
            // increase the current load
            if (secondsLeft.getValue() == 0 && end != -1 && currentLoad < end) {
<<<<<<< HEAD

                LOG.info("Ramping up...");
                LOG.info("[ramping up] Current load is " + currentLoad);
=======
                secondsLeft.setValue(interval);

>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
                currentLoad += rate;
                LOG.debug("Current load: " + currentLoad);

                if (currentLoad > end) {
                    currentLoad = end;
                }

                LOG.info("[ramping up] New load is " + currentLoad);
                secondsLeft.setValue(interval / 1000);
            }
        }

        private void rampDown(MutableLong secondsLeft) {
            if (currentLoad == end || ((oneAgentRate > 0) && (currentLoad - rate <= end - oneAgentRate))) {
<<<<<<< HEAD
                LOG.info("[ramp down] Execution finished...");

=======
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc
                finishExecution();
            }

            LOG.info("[rampDown] end is " + end + " secondsleft is " + secondsLeft.getValue() + " currentLoad is " +
                    currentLoad);

            // if 'interval' has passed and the currentLoad is still above 'end',
            // decrease the current load
            if (secondsLeft.getValue() == 0 && end != -1 && currentLoad > end) {
<<<<<<< HEAD
                LOG.info("[rampDown] end is " + end);
                LOG.info("Ramping down...");
                LOG.info("[ramping up] Current load is " + currentLoad);
=======
                secondsLeft.setValue(interval);
>>>>>>> a5a4297cce69a4a59f3ea89066219df2314daefc

                currentLoad -= rate;
                LOG.debug("Current load: " + currentLoad);

                if (currentLoad < end) {
                    currentLoad = end;
                }

                secondsLeft.setValue(interval / 1000);
            }
        }

        private void runRound() throws InterruptedException {
            ArrayList<AbstractTest> nextRound = new ArrayList<>();
            for (int i = 0; i < currentLoad; i++) {
                AbstractTest nextTest = Engine.getNextTest(phase.getTestSuite(),
                        phase.getCounts(),
                        engine.getEngineSync());
                if (null == nextTest) {
                    LOG.info("Constant load scheduler thread finished, because there were no more tests to execute.");
                    this.finishExecution();
                    return;
                }

                // Use a cache test if available
                AbstractTest localNextTest = testCache.getCachedValue(nextTest.getId());
                if(localNextTest == null) {
                    localNextTest = nextTest.clone();
                }

                nextRound.add(localNextTest);
            }

            for (int i = 0; i < currentLoad && !isFinished(); i++) {
                AsyncTestWorkerImpl worker = new AsyncTestWorkerImpl(nextRound.get(i), runMaps.get(i));
                try {
                    executorService.execute(worker);
                } catch (OutOfMemoryError e) {
                    if (!loggedWarning.getAndSet(true)) {
                        LOG.warn("The desired load could not be achieved. We are creating as many threads as possible.");
                    }
                    break;
                }
                synchronized (testWorkers) {
                    testWorkers.add(worker);
                }
            }
        }
    }

    public long getInitialDelay() {
        return this.initialDelay;
    }

    @Override
    public ExecutorService getExecutorService() {
        return executorService;
    }
}
