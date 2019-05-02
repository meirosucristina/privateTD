package com.adobe.qe.toughday.internal.core.distributedtd.tasks;

import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.Driver;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.DriverState;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MasterHeartbeatTask implements Runnable {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final DriverState driverState;
    private final HttpUtils httpUtils = new HttpUtils();

    public MasterHeartbeatTask(DriverState driverState) {
        this.driverState = driverState;
    }

    @Override
    public void run() {
        LOG.info(this.driverState.getHostname() + ": sending heartbeat message to master: " +
                Driver.getHealthPath(this.driverState.getPathForId(this.driverState.getMasterId())));

        HttpResponse driverResponse = this.httpUtils.sendHttpRequest(HttpUtils.GET_METHOD, "",
                Driver.getHealthPath(this.driverState.getPathForId(this.driverState.getMasterId())),
                HttpUtils.HTTP_REQUEST_RETRIES);

        if (driverResponse == null) {
            LOG.info(this.driverState.getHostname() + ": master failed to respond to heartbeat message. Master " +
                    "election process will be triggered soon.");
            // TODO: schedule master election
        }
    }
}
