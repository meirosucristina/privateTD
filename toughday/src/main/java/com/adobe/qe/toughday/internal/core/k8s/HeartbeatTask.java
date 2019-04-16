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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.adobe.qe.toughday.internal.core.k8s.HttpUtils.HEARTBEAT_PATH;
import static com.adobe.qe.toughday.internal.core.k8s.HttpUtils.URL_PREFIX;

public class HeartbeatTask implements Runnable {
    protected static final Logger LOG = LogManager.getLogger(Driver.class);
    private static final String PORT = "4567";

    private final ConcurrentHashMap<String, String> agents;
    private DistributedPhaseMonitor phaseMonitor;
    private Configuration configuration;
    private final HttpUtils httpUtils = new HttpUtils();

    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();

    public HeartbeatTask(ConcurrentHashMap<String, String> agents, DistributedPhaseMonitor phaseMonitor,
                         Configuration configuration) {
        this.agents = agents;
        this.phaseMonitor = phaseMonitor;
        this.configuration = configuration;
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
                    this.phaseMonitor.getExecutions()
                            .get(testName).put(agentName, doubleAgentCounts.get(testName).longValue()));

        } catch (Exception e) {
            System.out.println("[heartbeat] Failed to process heartbeat response from agent " + agentName + ". " +
                    "Error message: "  + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            for (String agentName : agents.keySet()) {
                String ipAddress = agents.get(agentName);
                String URI = URL_PREFIX + ipAddress + ":" + PORT + HEARTBEAT_PATH;

                HttpResponse agentResponse = httpUtils.sendHeartbeatRequest(URI, 3);
                processHeartbeatResponse(agentName, agentResponse);

                if (agentResponse == null) {
                    LOG.log(Level.INFO, "Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                    System.out.println("Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                    agents.remove(agentName);


                    if (!this.phaseMonitor.isPhaseExecuting()) {
                        continue;
                    }

                    if (this.taskBalancer.getState() == TaskBalancer.RebalanceState.EXECUTING) {
                        /* delay redistribution of tasks. Redistribution will be triggered after the current one is
                         * is finished
                         **/

                        this.taskBalancer.addInactiveAgent(agentName);
                        continue;
                    } else {
                        // TODO: schedule rabalance right now
                    }

                    // redistribute the work between the active agents
                    /*this.taskBalancer.rebalanceWork(this.phaseMonitor.getPhase(), this.phaseMonitor.getExecutionsPerTest(),
                            this.agents, this.configuration);
                    this.phaseMonitor.removeAgentFromExecutionsMap(agentId);*/

                    // TODO: change this when the new pod is able to resume the task
                    System.out.println("Removing agent " + agentName + " from map of executions. ");
                    this.phaseMonitor.removeRunningTask(agentName);
                }
            }

            System.out.println("[heartbeat] Total nr of executions " + this.phaseMonitor.getExecutionsPerTest().toString());

        } catch (Exception e) {
            // TODO: announce driver that heartbeat task must be rescheduled
            System.out.println("[heartbeat task] error: " + e.getMessage());
        }
    }
}
