package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.k8s.cluster.Driver;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.TaskBalancer;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
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

    private final TaskBalancer taskBalancer = new TaskBalancer();

    public HeartbeatTask(ConcurrentHashMap<String, String> agents, DistributedPhaseMonitor phaseMonitor,
                         Configuration configuration) {
        this.agents = agents;
        this.phaseMonitor = phaseMonitor;
        this.configuration = configuration;
    }

    private int executeHeartbeatRequest(HttpClient heartBeatHttpClient, HttpGet heartbeatRequest, String agentName) {
        try {
            HttpResponse agentResponse = heartBeatHttpClient.execute(heartbeatRequest);
            int responseCode = agentResponse.getStatusLine().getStatusCode();

            if (responseCode == 200) {
                // the agent has sent his statistic for executions/test => aggregate counts
                Gson gson = new Gson();
                String yamlCounts =  EntityUtils.toString(agentResponse.getEntity());

                try {
                    // gson treats numbers as double values by default
                    Map<String, Double> doubleAgentCounts = gson.fromJson(yamlCounts, Map.class);
                    // recently added agents might not execute tests yet
                    if (doubleAgentCounts.isEmpty()) {
                        return responseCode;
                    }

                    this.phaseMonitor.getExecutions().forEach((testName, executionsPerAgent) ->
                            this.phaseMonitor.getExecutions()
                                    .get(testName).put(agentName, doubleAgentCounts.get(testName).longValue()));

                } catch (Exception e) {
                    System.out.println("[heartbeat] Failed to process heartbeat response from agent " + agentName + ". " +
                            "Error message: "  + e.getMessage());
                }
            } else {
                System.out.println("[heartbeat] Failed to execute heartbeat request for agent " + agentName + ". " +
                        "Response code : "  + responseCode);
            }

            return responseCode;
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public void run() {
        try {
            for (String agentId : agents.keySet()) {
                CloseableHttpClient heartBeatHttpClient = HttpClientBuilder.create().build();

                String ipAddress = agents.get(agentId);
                String URI = URL_PREFIX + ipAddress + ":" + PORT + HEARTBEAT_PATH;
                // configure timeout limits
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectionRequestTimeout(1000)
                        .setConnectTimeout(1000)
                        .setSocketTimeout(1000)
                        .build();
                HttpGet heartbeatRequest = new HttpGet(URI);
                heartbeatRequest.setConfig(requestConfig);

                int retrial = 3;
                int responseCode = 0;
                while (responseCode != 200 && retrial > 0) {
                    responseCode = executeHeartbeatRequest(heartBeatHttpClient, heartbeatRequest, agentId);
                    retrial -= 1;
                }

                if (retrial <= 0) {
                    LOG.log(Level.INFO, "Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                    System.out.println("Agent with ip " + ipAddress + " failed to respond to heartbeat request.");
                    agents.remove(agentId);


                    if (!this.phaseMonitor.isPhaseExecuting()) {
                        continue;
                    }

                    // redistribute the work between the active agents
                    this.taskBalancer.rebalanceWork(this.phaseMonitor.getPhase(), this.phaseMonitor.getExecutionsPerTest(),
                            this.agents, new HashMap<>(), this.configuration);
                    this.phaseMonitor.removeAgentFromExecutionsMap(agentId);

                    // TODO: change this when the new pod is able to resume the task
                    System.out.println("Removing agent " + agentId + " from map of executions. ");
                    this.phaseMonitor.removeRunningTask(agentId);
                }

                try {
                    heartBeatHttpClient.close();
                } catch (IOException e) {
                    LOG.warn("HeartBeat apache http client could not be closed.");
                }
            }

            System.out.println("[heartbeat] Total nr of executions " + this.phaseMonitor.getExecutionsPerTest().toString());

        } catch (Exception e) {
            // TODO: announce driver that heartbeat task must be rescheduled
            System.out.println("[heartbeat task] error: " + e.getMessage());
        }
    }
}
