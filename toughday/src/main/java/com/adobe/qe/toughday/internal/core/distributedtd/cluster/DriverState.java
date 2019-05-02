package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

public class DriverState {
    // Documentation says that each service has the following DNS A record: my-svc.my-namespace.svc.cluster.local
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final String hostname;
    private static final String SVC_NAME = "driver";
    private final Queue<String> agents = new ConcurrentLinkedQueue<>();
    private final Queue<Integer> inactiveDrivers = new ConcurrentLinkedQueue<>();
    private final int id;
    private int masterId = 0;
    private int nrDrivers;
    private Configuration driverConfig;
    private State currentState;
    private int nrVotes = 0;

    public enum State {
        CHOOSING_MASTER,
        PICKED_MASTER,
        SLAVE,
        MASTER
    }

    public String getPathForId(int id) {
        return SVC_NAME + "-" + id + "." + SVC_NAME + "." +
                this.driverConfig.getDistributedConfig().getClusterNamespace() + ".svc.cluster.local";
    }

    public void increaseNumberOfVotes() {
        this.nrVotes += 1;
    }

    public int getNrVotes() {
        return this.nrVotes;
    }

    public void resetNrVotes() {
        this.nrVotes = 0;
    }

    public DriverState(String hostname, Configuration driverConfig) {
        this.driverConfig = driverConfig;
        this.hostname = hostname;
        LOG.info("Hostname is " + this.hostname);
        String name_pattern = "driver-[0-9]+\\." + SVC_NAME + "\\." + this.driverConfig.getDistributedConfig().getClusterNamespace() +
                "\\.svc\\.cluster\\.local";

        if (!Pattern.matches(name_pattern, hostname)) {
            throw new IllegalStateException("Driver's name should respect the following format: driver-<id>.<svc_name>.<namespace>.svc.cluster.local");
        }

        String simpleHostname = hostname.substring(0, hostname.indexOf("." + SVC_NAME + "." +
                this.driverConfig.getDistributedConfig().getClusterNamespace()));
        id = Integer.parseInt(simpleHostname.substring(simpleHostname.lastIndexOf("-") + 1));
        System.getenv().forEach((key, value) -> System.out.println(key + "   " + value));
        this.nrDrivers = Integer.parseInt(System.getenv("NR_DRIVERS"));

        LOG.info("Nr of drivers running in the cluster: " + this.nrDrivers);
        LOG.info("ID is " + this.id);

        if (this.id == masterId) {
            LOG.info("Running as MASTER");
            this.currentState = State.MASTER;
        } else {
            LOG.info("Running as SLAVE");
            this.currentState = State.SLAVE;
        }
    }

    public String getHostname() {
        return this.hostname;
    }

    public int getId() {
        return this.id;
    }

    public int getMasterId() {
        return this.masterId;
    }

    public int getNrDrivers() {
        return this.nrDrivers;
    }

    public Queue<String> getRegisteredAgents() {
        return this.agents;
    }

    public void registerAgent(String agentIdentifier) {
        this.agents.add(agentIdentifier);
    }

    public void addInactiveDriver(int driverIdentifier) {
        this.inactiveDrivers.add(driverIdentifier);
    }

    public Queue<Integer> getInactiveDrivers() {
        return this.inactiveDrivers;
    }

    public void removeInactiveDriver(int driverIdentifier) {
        this.inactiveDrivers.remove(driverIdentifier);
    }

    public void removeAgent(String agentIdentifier) {
        this.agents.remove(agentIdentifier);
    }

    public Configuration getDriverConfig() {
        return this.driverConfig;
    }

    public State getCurrentState() {
        return this.currentState;
    }

    public void setCurrentState(State state) {
        this.currentState = state;
    }

    public void setMasterId(int id) {
        this.masterId = id;
    }
}
