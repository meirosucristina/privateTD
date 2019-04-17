package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.k8s.cluster.Driver;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.TaskBalancer;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.adobe.qe.toughday.internal.core.k8s.HttpUtils.HEARTBEAT_PATH;
import static com.adobe.qe.toughday.internal.core.k8s.HttpUtils.URL_PREFIX;

public class HeartbeatTask implements Runnable {
    protected static final Logger LOG = LogManager.getLogger(Driver.class);
    private static final String PORT = "4567";

    private final ConcurrentHashMap<String, String> agents;
    private DistributedPhaseMonitor phaseMonitor;
    private Configuration configuration;
    private Configuration driverConfiguration;
    private final HttpUtils httpUtils = new HttpUtils();

    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();

    public HeartbeatTask(ConcurrentHashMap<String, String> agents, DistributedPhaseMonitor phaseMonitor,
                         Configuration configuration, Configuration driverConfiguration) {
        this.agents = agents;
        this.phaseMonitor = phaseMonitor;
        this.configuration = configuration;
        this.driverConfiguration = driverConfiguration;
    }

    private void processHeartbeatResponse(String agentName, HttpResponse agentResponse) {
        if (agentResponse == null) {
            return;
        }

        try {
            // the agent has sent his statistic for executions/test => aggregate counts
            Gson gson = new Gson();
            String yamlCounts =  EntityUtils.toString(agentResponse.getEntity());

            // gson treats numbers as double values by default
            Map<String, Double> doubleAgentCounts = gson.fromJson(yamlCounts, Map.class);
            System.out.println("[Heartbeat task] Received from agent " + agentName + "(" + agents.get(agentName) + ") : " + doubleAgentCounts.toString());

            // recently added agents might not execute tests yet
            if (doubleAgentCounts.isEmpty()) {
                return;
            }

            this.phaseMonitor.getExecutions().forEach((testName, executionsPerAgent) ->
                    executionsPerAgent.put(agentName, doubleAgentCounts.get(testName).longValue()));

        } catch (Exception e) {
            System.out.println("[heartbeat] Failed to process heartbeat response from agent " + agentName + ". " +
                    "Error message: "  + e.getMessage());
        }
    }

    @Override
    public void run() {
        Map<String, String> activeAgents = new HashMap<>(agents);
        this.taskBalancer.getInactiveAgents().forEach(activeAgents::remove);

        System.out.println("[heartbeat task] active agents " + activeAgents.keySet().toString());
        System.out.println("[heartbeat task] Inactive agents is " + this.taskBalancer.getInactiveAgents().toString());

        try {
            for (String agentName : activeAgents.keySet()) {
                String ipAddress = agents.get(agentName);
                String URI = URL_PREFIX + ipAddress + ":" + PORT + HEARTBEAT_PATH;

                HttpResponse agentResponse = httpUtils.sendHeartbeatRequest(URI, 3);
                processHeartbeatResponse(agentName, agentResponse);

                if (agentResponse == null) {
                    LOG.log(Level.INFO, "Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                    System.out.println("Agent with ip " + ipAddress + " failed to respond to heartbeat request.");

                    if (!this.phaseMonitor.isPhaseExecuting()) {
                        agents.remove(agentName);
                        continue;
                    }

                    this.taskBalancer.addInactiveAgent(agentName);

                    if (this.taskBalancer.getState() == TaskBalancer.RebalanceState.EXECUTING) {
                        /* delay redistribution of tasks. Redistribution will be triggered after the current one is
                         * finished
                         **/
                        System.out.println("[heartbeat task] Agent " + agentName + "(" + agents.get(agentName) + ") is no longer " +
                                " available. Delayed redistribution process will exclude this agent.");
                        this.taskBalancer.setState(TaskBalancer.RebalanceState.RESCHEDULED_REQUIRED);
                        //this.taskBalancer.addInactiveAgent(agentName);
                        continue;
                    } else if (this.taskBalancer.getState() != TaskBalancer.RebalanceState.SCHEDULED) {
                        // TODO: schedule rabalance right now
                        this.taskBalancer.setState(TaskBalancer.RebalanceState.SCHEDULED);
                        System.out.println("[heartbeat task] Scheduling rebalance process to start in 3 seconds...");

                        // we should no longer send heartbeat requests to this agent
                        // agents.remove(agentName);
                        System.out.println("[heartbeat task] " + agentName + " was removed from agents.");

                        // schedule rebalance process
                        ScheduledFuture<Map<String, Future<HttpResponse>>> scheduledFuture =
                                this.taskBalancer.getRebalanceScheduler()
                                        .schedule(() -> taskBalancer.rebalanceWork(this.phaseMonitor, this.agents, this.configuration,
                                                    this.driverConfiguration.getK8SConfig(), this.phaseMonitor.getPhaseStartTime()),
                                                this.driverConfiguration.getK8SConfig().getRedistributionWaitTimeInSeconds(),
                                                TimeUnit.SECONDS);

                        System.out.println("[hearbeat task] Scheduled ack...");
                    }

                    // TODO: change this when the new pod is able to resume the task
                    System.out.println("Removing agent " + agentName + " from map of executions. ");
                    this.phaseMonitor.removeRunningTask(agentName);
                }
            }

            System.out.println("[heartbeat] Total nr of executions " + this.phaseMonitor.getExecutionsPerTest().toString());

        } catch (Exception e) {
            e.printStackTrace();
            // TODO: announce driver that heartbeat task must be rescheduled
            System.out.println("[heartbeat task] error: " + e.getMessage());
        }
    }
}
