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

import com.adobe.qe.toughday.api.core.*;
import com.adobe.qe.toughday.internal.core.*;
import com.adobe.qe.toughday.api.annotations.Description;
import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.engine.*;
import com.adobe.qe.toughday.internal.core.k8s.RebalanceInstructions;
import com.adobe.qe.toughday.internal.core.k8s.RunModeBalancer;
import com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes.NormalRunModeBalancer;
import com.adobe.qe.toughday.internal.core.k8s.splitters.runmodes.NormalRunModeSplitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

@Description(desc = "Runs tests normally.")
public class Normal implements RunMode, Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(Normal.class);

    private static final int EPS = 1;

    private static final String DEFAULT_CONCURRENCY_STRING = "200";
    private static final int DEFAULT_CONCURRENCY = Integer.parseInt(DEFAULT_CONCURRENCY_STRING);

    private static final String DEFAULT_WAIT_TIME_STRING = "300";
    private static final long DEFAULT_WAIT_TIME = Long.parseLong(DEFAULT_WAIT_TIME_STRING);

    private static final String DEFAULT_INTERVAL_STRING = "1s";
    private static final long DEFAULT_INTERVAL = 1000;

    private ExecutorService testsExecutorService;
    private ScheduledExecutorService addWorkerScheduler = Executors.newScheduledThreadPool(1);
    private ScheduledExecutorService removeWorkerScheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> scheduledFuture = null;

    private final List<AsyncTestWorker> testWorkers = Collections.synchronizedList(new LinkedList<>());
    private final List<RunMap> runMaps = new ArrayList<>();
    private Phase phase;

    private int start = DEFAULT_CONCURRENCY;
    private int end = DEFAULT_CONCURRENCY;
    private int concurrency = DEFAULT_CONCURRENCY;
    private int rate;
    private long waitTime = DEFAULT_WAIT_TIME;
    private long interval = DEFAULT_INTERVAL;
    private int activeThreads = 0;
    private long initialDelay = 0;

    private RunContext context = null;
    private RunModeSplitter<Normal> runModeSplitter = new NormalRunModeSplitter();
    private final RunModeBalancer<Normal> normalRunModeBalancer = new NormalRunModeBalancer();
    private Engine engine;


    public Normal() {
        /* this is required when running TD distributed on K8s because scheduled task might be cancelled and
         * rescheduled when rebalancing the work between the agents.
         */
        ScheduledThreadPoolExecutor scheduledPoolExecutor = (ScheduledThreadPoolExecutor) addWorkerScheduler;
        scheduledPoolExecutor.setRemoveOnCancelPolicy(true);

        scheduledPoolExecutor = (ScheduledThreadPoolExecutor) removeWorkerScheduler;
        scheduledPoolExecutor.setRemoveOnCancelPolicy(true);
    }

    @ConfigArgGet(redistribute = true)
    public int getConcurrency() {
        return concurrency;
    }

    @ConfigArgSet(required = false, desc = "The number of concurrent threads that Tough Day will use.",
            defaultValue = DEFAULT_CONCURRENCY_STRING, order = 5)
    public void setConcurrency(String concurrencyString) {
        checkNotNegative(Long.parseLong(concurrencyString), "concurrency");
        this.concurrency = Integer.parseInt(concurrencyString);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @ConfigArgGet
    public long getWaitTime() {
        return waitTime;
    }

    @ConfigArgSet(required = false, desc = "The wait time between two consecutive test runs for a specific thread. Expressed in milliseconds.",
            defaultValue = DEFAULT_WAIT_TIME_STRING, order = 7)
    public void setWaitTime(String waitTime) {
        checkNotNegative(Long.parseLong(waitTime), "waittime");
        this.waitTime = Integer.parseInt(waitTime);
    }

    @ConfigArgGet()
    public int getStart() {
        return start;
    }

    @ConfigArgSet(required = false, desc = "The number of threads to start ramping up from. Will rise to the number specified by \"concurrency\".",
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

    @ConfigArgSet(required = false, desc = "The number of threads added per time unit. When it equals -1, it means it is not set.",
            defaultValue = "-1")
    public void setRate(String rate) {
        if (!rate.equals("-1")) {
            checkNotNegative(Long.parseLong(rate), "rate");
        }
        this.rate = Integer.valueOf(rate);
    }

    @ConfigArgGet(redistribute = true)
    public String getInterval() {
        return String.valueOf(this.interval / 1000) + 's';
    }

    public void setInitialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
    }

    @ConfigArgSet(required = false, desc = "Used with rate to specify the time interval to add threads.",
            defaultValue = DEFAULT_INTERVAL_STRING)
    public void setInterval(String interval) {
        this.interval = GlobalArgs.parseDurationToSeconds(interval) * 1000;
    }

    @ConfigArgGet(redistribute = true)
    public int getEnd() {
        return end;
    }

    @ConfigArgSet(required = false, desc = "The maximum number of threads the thread pool can reach.", defaultValue = "-1")
    public void setEnd(String end) {
        if (!end.equals("-1")) {
            checkNotNegative(Long.parseLong(end), "end");
        }
        this.end = Integer.valueOf(end);
    }

    public int getActiveThreads() {
        return activeThreads;
    }

    public ScheduledFuture<?> getScheduledFuture() {
        return this.scheduledFuture;
    }

    public ScheduledExecutorService getAddWorkerScheduler() {
        return this.addWorkerScheduler;
    }

    public ScheduledExecutorService getRemoveWorkerScheduler() {
        return this.removeWorkerScheduler;
    }

    public Engine getEngine() {
        return this.engine;
    }

    private void checkNotNegative(long param, String property) {
        if (param < 0) {
            throw new IllegalArgumentException("Property " + property + " incorrectly configured as negative.");
        }
    }

    private void checkInvalidArgs() {
        if ((start != -1 && end == -1) || (start == -1 && end != -1)) {
            throw new IllegalArgumentException("Cannot configure only one limit (start/end) for Normal mode.");
        }

        if (isVariableConcurrency() && concurrency != DEFAULT_CONCURRENCY) {
            throw new IllegalArgumentException("Normal mode cannot be configured with both start/end and concurrency.");
        }
    }

    public boolean isVariableConcurrency() {
        return start != -1 && end != -1;
    }

    @Override
    public void runTests(Engine engine) {
        checkInvalidArgs();
        this.engine = engine;

        this.phase = engine.getCurrentPhase();
        TestSuite testSuite = phase.getTestSuite();
        testsExecutorService = Executors.newCachedThreadPool();
        ((ThreadPoolExecutor)testsExecutorService).setKeepAliveTime(1, TimeUnit.SECONDS);

        // if no rate was provided, we'll create/remove one user at fixed rate,
        // namely every 'interval' milliseconds
        if (isVariableConcurrency()) {  // if start and end were provided
            if (rate == -1) {
                interval = (long)Math.floor(1000.0 * (GlobalArgs.parseDurationToSeconds(phase.getDuration()) - EPS)
                        / (start < end? end - start : start - end));
                rate = 1;
            }

            concurrency = start;
        }

        // Execute the test worker threads
        // if start was provided, then it will create 'start' (whose value was
        // assigned to 'concurrency') workers to begin with
        // otherwise, it will create 'concurrency' workers
        for (int i = 0; i < concurrency; i++) {
            createAndExecuteWorker(engine, testSuite);
        }

        // execute 'rate' workers every 'interval'
        rampUp(engine, testSuite);

        // interrupt 'rate' workers every 'interval'
        rampDown();
    }



    public void createAndExecuteWorker(Engine engine, TestSuite testSuite) {
        AsyncTestWorkerImpl testWorker = new AsyncTestWorkerImpl(engine, phase, testSuite, phase.getPublishMode().getRunMap().newInstance());
        synchronized (testWorkers) {
            testWorkers.add(testWorker);
            activeThreads++;
        }
        synchronized (runMaps) {
            runMaps.add(testWorker.getLocalRunMap());
        }

        try {
            testsExecutorService.execute(testWorker);
        } catch (OutOfMemoryError e) {
            LOG.warn("Could not create the required number of threads. Number of created threads : " + String.valueOf(activeThreads) + ".");
        }
    }

    public Runnable getRampUpRunnable(Engine engine, TestSuite testSuite) {
        return () -> {
            for (int i = 0; i < rate; ++i) {
                // if all the workers have been created
                if (activeThreads >= end) {
                    addWorkerScheduler.shutdownNow();

                    // mark workers as finished
                    testWorkers.forEach(AsyncEngineWorker::finishExecution);

                    break;
                } else {
                    createAndExecuteWorker(engine, testSuite);
                }
            }
        };
    }

    public Runnable getRampDownRunnable() {
        return () -> {
            Iterator<AsyncTestWorker> testWorkerIterator = testWorkers.iterator();
            int toRemove = rate;

            while (testWorkerIterator.hasNext()) {
                // if all the workers have been removed
                if (activeThreads <= end) {
                    removeWorkerScheduler.shutdownNow();

                    // mark all the workers as finished, so the timeout checker will stop the execution
                    testWorkers.forEach(AsyncEngineWorker::finishExecution);

                    break;
                } else {
                    AsyncTestWorker testWorker = testWorkerIterator.next();
                    testWorker.finishExecution();

                    // remove the stopped worker
                    testWorkerIterator.remove();
                    --toRemove;
                    --activeThreads;

                    // if rate users have been removed
                    if (toRemove == 0) {
                        break;
                    }
                }
            }
        };
    }

    private void rampUp(Engine engine, TestSuite testSuite) {
        // every 'interval' milliseconds, we'll create 'rate' workers
        if (start < end) {
            this.scheduledFuture = addWorkerScheduler.scheduleAtFixedRate(getRampUpRunnable(engine, testSuite),
                    this.initialDelay, interval, TimeUnit.MILLISECONDS);
        }
    }

    private void rampDown() {
        // every 'interval' milliseconds, we'll stop 'rate' workers
        if (end < start) {
            removeWorkerScheduler.scheduleAtFixedRate(getRampDownRunnable(), initialDelay, interval, TimeUnit.MILLISECONDS);
        }
    }

    public RunContext getRunContext() {
        if (context == null) {
            context = new RunContext() {
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
                    for(AsyncTestWorker testWorker : testWorkers) {
                        if (!testWorker.isFinished())
                            return false;
                    }
                    return true;
                }
            };
        }

        return context;
    }

    @Override
    public RunModeBalancer<Normal> getRunModeBalancer() {
        return this.normalRunModeBalancer;
    }

    /*private void processPropertyChange(String property, String newValue) {
        if (property.equals("concurrency") && !isVariableConcurrency()) {
            System.out.println("[rebalance processor] Processing concurrency change");
            long newConcurrency = Long.parseLong(newValue);
            long difference = this.concurrency - newConcurrency;

            System.out.println("[rebalance processsor] concurrency difference is " + difference);

            if (difference > 0) {
                // kill some test workers
                for (int i = 0; i < difference; i++) {
                    this.testWorkers.get(i).finishExecution();
                    this.testWorkers.remove(i);
                    System.out.println("[rebalance processor] Finished test worker " + this.testWorkers.get(i).getWorkerThread().getId());
                }
            } else {
                // create a few more test workers
                for (int i = 0; i < Math.abs(difference); i++) {
                    System.out.println("[rebalance processor] Creating a new test worker...");
                    createAndExecuteWorker(engine, engine.getCurrentPhase().getTestSuite());
                }
            }

            System.out.println("[rebalance processor] Successfully updated the state to respect the new value of concurrency.");
        }
    }

    @Override
    public void processRebalanceInstructions(RebalanceInstructions rebalanceInstructions) {
        if (this.isVariableConcurrency()) {
            *//* We must cancel the scheduled task and reschedule it with the new values for 'period' and
             * initial delay.
             *//*
            boolean cancelled = this.scheduledFuture.cancel(true);
            if (!cancelled) {
                System.out.println("[rebalance processor - run mode] task could not be cancelled.");
                return;
            }

            System.out.println("[rebalance processor - run mode] successfully cancelled task.");
        }

        Map<String, String> runModeProperties = rebalanceInstructions.getRunModeProperties();
        runModeProperties.forEach(this::processPropertyChange);

        // TODO: should we wait for all the agents to confirm the interruption on the scheduled task?
        // reschedule the task
        if (this.isVariableConcurrency()) {
            if (start < end) {
                this.addWorkerScheduler.scheduleAtFixedRate(getRampUpRunnable(engine, engine.getCurrentPhase().getTestSuite()),
                        this.initialDelay, this.interval, TimeUnit.MILLISECONDS);
                System.out.println("[rebalance processor - run mode] successfully rescheduled ramp up with interval " +
                        this.interval + " and initial delay " + this.initialDelay);
            } else if (start > end) {
                this.removeWorkerScheduler.scheduleAtFixedRate(getRampDownRunnable(), this.initialDelay, this.interval,
                        TimeUnit.MILLISECONDS);
                System.out.println("[rebalance processor - run mode] successfully rescheduled ramp down with interval " +
                        this.interval + " and initial delay " + this.initialDelay);
            }
        }
    }*/

    @Override
    public RunModeSplitter<Normal> getRunModeSplitter() {
        return this.runModeSplitter;
    }

    @Override
    public void finishExecutionAndAwait() {
        if (!addWorkerScheduler.isShutdown()) {
            addWorkerScheduler.shutdownNow();
        }

        if(!removeWorkerScheduler.isShutdown()) {
            removeWorkerScheduler.shutdownNow();
        }

        synchronized (testWorkers) {
            for (AsyncTestWorker testWorker : testWorkers) {
                testWorker.finishExecution();
            }
        }

        boolean allExited = false;
        while(!allExited) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
            allExited = true;
            synchronized (testWorkers) {
                for (AsyncTestWorker testWorker : testWorkers) {
                    if (!testWorker.hasExited()) {
                        if(!testWorker.getMutex().tryLock()) {
                            continue;
                        }
                        allExited = false;

                        testWorker.getWorkerThread().interrupt();
                        testWorker.getMutex().unlock();
                    }
                }
            }
        }
    }

    @Override
    public ExecutorService getExecutorService() {
        return testsExecutorService;
    }

    private class AsyncTestWorkerImpl extends AsyncTestWorker {
        protected final Engine engine;
        private HashMap<TestId, AbstractTest> localTests;
        private TestSuite testSuite;
        private RunMap localRunMap;
        private Phase phase;
        private boolean exited = false;

        /**
         * Constructor
         *
         * @param engine
         * @param testSuite   the test suite
         * @param localRunMap a deep clone a the global run map.
         */
        public AsyncTestWorkerImpl(Engine engine, Phase phase, TestSuite testSuite, RunMap localRunMap) {
            this.phase = phase;
            this.engine = engine;
            this.testSuite = testSuite;
            localTests = new HashMap<>();
            for(AbstractTest test : testSuite.getTests()) {
                AbstractTest localTest = test.clone();
                localTests.put(localTest.getId(), localTest);
            }
            this.localRunMap = localRunMap;
        }

        /**
         * Method for running tests.
         */
        @Override
        public void run() {
            workerThread = Thread.currentThread();
            LOG.debug("Thread running: " + workerThread);
            mutex.lock();
            try {
                while(!isFinished()) {
                    currentTest = Engine.getNextTest(this.testSuite, phase.getCounts(), engine.getEngineSync());
                    // if no test available, finish
                    if (null == currentTest) {
                        LOG.info("Thread " + workerThread + " died! :(");
                        this.finishExecution();

                        continue;
                    }

                    //get the worker's local test to run
                    currentTest = localTests.get(currentTest.getId());

                    // else, continue with the run

                    AbstractTestRunner runner = RunnersContainer.getInstance().getRunner(currentTest);

                    lastTestStart = System.nanoTime();
                    mutex.unlock();
                    try {
                        runner.runTest(currentTest, localRunMap);
                    } catch (Throwable e) {
                        LOG.warn("Exceptions from tests should not reach this point", e);
                    }
                    mutex.lock();
                    Thread.interrupted();
                    Thread.sleep(waitTime);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("InterruptedException(s) should not reach this point", e);
            } catch (Throwable e) {
                LOG.error("Unexpected exception caught", e);
            } finally {
                mutex.unlock();
                this.exited = true;
            }
        }

        public RunMap getLocalRunMap() {
            return localRunMap;
        }

        @Override
        public boolean hasExited() {
            return exited;
        }
    }

    public long getInitialDelay() {
        return this.initialDelay;
    }


 }
