package com.adobe.qe.toughday.internal.core.distributedtd.tasks;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseInfo;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.Driver;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class HeartbeatTask implements Runnable {
    protected static final Logger LOG = LogManager.getLogger(Driver.class);

    private final ConcurrentHashMap<String, String> agents;
    private DistributedPhaseInfo distributedPhaseInfo;
    private Configuration configuration;
    private Configuration driverConfiguration;
    private final HttpUtils httpUtils = new HttpUtils();

    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();

    public HeartbeatTask(ConcurrentHashMap<String, String> agents, DistributedPhaseInfo distributedPhaseInfo,
                         Configuration configuration, Configuration driverConfiguration) {
        this.agents = agents;
        this.distributedPhaseInfo = distributedPhaseInfo;
        this.configuration = configuration;
        this.driverConfiguration = driverConfiguration;
    }

    private void processHeartbeatResponse(String agentName, HttpResponse agentResponse) throws IOException {
        // the agent has sent his statistic for executions/test => aggregate counts
        Gson gson = new Gson();
        String yamlCounts =  EntityUtils.toString(agentResponse.getEntity());

        // gson treats numbers as double values by default
        Map<String, Double> doubleAgentCounts = gson.fromJson(yamlCounts, Map.class);
        LOG.info("[heartbeat] Received execution state from agent " + agentName +
                "(" + agents.get(agentName) + ") : " + doubleAgentCounts.toString());

        // recently added agents might not execute tests yet
        if (doubleAgentCounts.isEmpty()) {
            return;
        }

        this.distributedPhaseInfo.getExecutions().forEach((testName, executionsPerAgent) ->
                executionsPerAgent.put(agentName, doubleAgentCounts.get(testName).longValue()));
    }

    @Override
    public void run() {
        Map<String, String> activeAgents = new HashMap<>(agents);
        // remove agents which previously failed to respond to heartbeat request
        this.taskBalancer.getInactiveAgents().forEach(activeAgents::remove);

        for (String agentName : activeAgents.keySet()) {
            String ipAddress = agents.get(agentName);

            String URI = HttpUtils.getHeartbeatPath(ipAddress);
            HttpResponse agentResponse = httpUtils.sendHeartbeatRequest(URI, 3);
            if (agentResponse != null) {
                try {
                    processHeartbeatResponse(agentName, agentResponse);
                } catch (IOException e) {
                    // skip this for now.
                }
                continue;
            }

            LOG.log(Level.INFO, "Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
            if (!this.distributedPhaseInfo.isPhaseExecuting()) {
                agents.remove(agentName);
                continue;
            }

            this.taskBalancer.addInactiveAgent(agentName);
            // we should not wait for task completion since the agent running it left the cluster
            this.distributedPhaseInfo.removeRunningTask(agentName);

            if (this.taskBalancer.getState() == TaskBalancer.RebalanceState.EXECUTING) {
                LOG.info("[Driver] Redistribution will be triggered again after the current one is finished because " +
                        "agent" + agentName + "(" + agents.get(agentName) + ") became unavailable." );
                this.taskBalancer.setState(TaskBalancer.RebalanceState.RESCHEDULED_REQUIRED);

            } else if (this.taskBalancer.getState() != TaskBalancer.RebalanceState.SCHEDULED) {
                this.taskBalancer.setState(TaskBalancer.RebalanceState.SCHEDULED);
                System.out.println("[Driver] Scheduling work redistribution process to start in " +
                        this.driverConfiguration.getDistributedConfig().getRedistributionWaitTimeInSeconds() + "seconds.");

                this.taskBalancer.getRebalanceScheduler()
                        .schedule(() -> taskBalancer.rebalanceWork(this.distributedPhaseInfo, this.agents, this.configuration,
                                    this.driverConfiguration.getDistributedConfig(), this.distributedPhaseInfo.getPhaseStartTime()),
                                this.driverConfiguration.getDistributedConfig().getRedistributionWaitTimeInSeconds(),
                                TimeUnit.SECONDS);
            }
        }

        LOG.info("[driver] Number of executions per test: " + this.distributedPhaseInfo.getExecutionsPerTest());
    }
}
