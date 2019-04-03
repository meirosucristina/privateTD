package com.adobe.qe.toughday.internal.core.engine;

public abstract class AbstractRunContext implements RunMode.RunContext {

    /**
     * Method responsible for interrupting all the active workers of the run mode.
     */
    public void interruptWorkers() {
        this.getTestWorkers().forEach(worker -> {
            worker.getStateLock().lock();
            worker.setState(AsyncTestWorker.State.INTERRUPTED);
            worker.getStateLock().unlock();
        });
    }
}
