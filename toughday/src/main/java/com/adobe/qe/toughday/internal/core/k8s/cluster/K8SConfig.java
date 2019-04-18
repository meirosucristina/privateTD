package com.adobe.qe.toughday.internal.core.k8s.cluster;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;

/**
 * Contains all the configurable arguments when running TD distributed on Kubernetes.
 */
public class K8SConfig {
    private static final String DEFAULT_HEARTBEAT_INTERVAL = "5s";
    private static final String DEFAULT_REDISTRIBUTION_WAIT_TIME = "3s";

    private boolean k8sAgent = false;
    private boolean k8sdriver = false;
    private String driverIp = null;
    private String heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private String redistributionWaitTime = DEFAULT_REDISTRIBUTION_WAIT_TIME;

    @ConfigArgSet(required = false, desc = "The public ip address of the cluster. The driver" +
            " service must be accessible at this address. This property is required when running in distributed mode.")
    public void setDriverIp(String driverIp) {
        this.driverIp = driverIp;
    }

    @ConfigArgGet
    public String getDriverIp() {
        return this.driverIp;
    }

    @ConfigArgGet
    public boolean getK8sAgent() { return this.k8sAgent; }

    @ConfigArgSet(required = false, defaultValue = "false", desc = "If true, TD runs as a K8s agent, waiting to receive" +
            " a task from the driver.")
    public void setK8sAgent(String k8sAgent) {
        this.k8sAgent = Boolean.parseBoolean(k8sAgent);
    }

    @ConfigArgGet
    public boolean getK8sdriver() {
        return this.k8sdriver;
    }

    @ConfigArgSet(required = false, defaultValue = "false", desc = "If true, TD runs as a driver in the cluster," +
            " distributing the work between the agents.")
    public void setK8sdriver(String k8sdriver) {
        this.k8sdriver = Boolean.parseBoolean(k8sdriver);
    }

    @ConfigArgSet(required = false, defaultValue = DEFAULT_HEARTBEAT_INTERVAL, desc = "Period of time for sending " +
            "heartbeat messages to the agents in the cluster.")
    public void setHeartbeatInterval(String heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    @ConfigArgGet
    public String getHeartbeatInterval() {
        return this.heartbeatInterval;
    }

    @ConfigArgSet(required = false, defaultValue = DEFAULT_REDISTRIBUTION_WAIT_TIME, desc = "The minimum amount of time " +
            " to wait before scheduling work redistribution if required.")
    public void setRedistributionWaitTime(String redistributionWaitTime) {
        this.redistributionWaitTime = redistributionWaitTime;
    }

    @ConfigArgGet
    public String getRedistributionWaitTime() {
        return this.redistributionWaitTime;
    }

    public long getRedistributionWaitTimeInSeconds() {
        return GlobalArgs.parseDurationToSeconds(this.redistributionWaitTime);
    }

    public long getHeartbeatIntervalInSeconds() {
        return GlobalArgs.parseDurationToSeconds(this.heartbeatInterval);
    }

    public void merge(K8SConfig other) {
        this.setHeartbeatInterval(other.getHeartbeatInterval());
        this.setRedistributionWaitTime(other.redistributionWaitTime);
    }
}
