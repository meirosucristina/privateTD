package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.*;

import static com.adobe.qe.toughday.internal.core.engine.Engine.installToughdayContentPackage;
import static spark.Spark.*;

/**
 * Driver component for K8s cluster.
 */
public class Driver {

    private final static String URL_PREFIX = "http://";
    private final static String SUBMIT_TASK_PATH = "/submitTask";
    private final static String PORT = "4567";
    private final static String REGISTRATION_PARAM = ":ipAddress";
    private final static String REGISTER_PATH = "/registerAgent/" + REGISTRATION_PARAM;
    private final static String EXECUTION_PATH = "/submitConfig";

    private final Map<String, String> agents = new HashMap<>();

    private void handleToughdaySampleContent(Configuration configuration) {
        GlobalArgs globalArgs = configuration.getGlobalArgs();

        if (globalArgs.getInstallSampleContent() && !globalArgs.getDryRun()) {
            //printConfiguration(configuration, new PrintStream(new Engine.LogStream(LOG)));
            try {
                installToughdayContentPackage(globalArgs);
                globalArgs.setInstallSampleContent("false");

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleExecutionRequest(Configuration configuration) {
        handleToughdaySampleContent(configuration);

        HttpClient httpClient = HttpClientBuilder.create().build();

        TaskPartitioner taskPartitioner = new TaskPartitioner();
        configuration.getPhases().forEach(phase -> {
            try {
                Map<String, Phase> tasks = taskPartitioner.splitPhase(phase, new ArrayList<>(agents.keySet()));

                agents.keySet().forEach(agentHostname -> {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentHostname)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    System.out.println("------------TASK IS--------------\n" + yamlTask);

                    try {
                        /* build HTTP query with yaml configuration as body */
                        HttpPost taskRequest = new HttpPost(agents.get(agentHostname));
                        StringEntity params = new StringEntity(yamlTask);
                        taskRequest.setEntity(params);
                        taskRequest.setHeader("Content-type", "text/plain");

                        /* submit request and check response code */
                        HttpResponse agentResponse = httpClient.execute(taskRequest);

                        System.out.println("Respone code from agent is " + agentResponse.getStatusLine().getStatusCode());

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        });
    }

    public void run() {
        /* expose http endpoint for running TD with the given configuration */
        post(EXECUTION_PATH, ((request, response) -> {
            String yamlConfiguration = request.body();
            Configuration configuration = new Configuration(yamlConfiguration);

            new Thread() {
                public synchronized void run() {
                    handleExecutionRequest(configuration);
                }
            }.start();


            return "Driver finished execution.";
        }));

        /* expose http endpoint for registering new agents to the cluster */
        get(REGISTER_PATH, (request, response) -> {
            String agentHostname = request.params(REGISTRATION_PARAM).replaceFirst(":", "");
            agents.put(agentHostname, URL_PREFIX + agentHostname + ":" + PORT + SUBMIT_TASK_PATH);

            System.out.println("For agent " + agentHostname + " the URI is " + agents.get(agentHostname));

            return "";
        });


        /* wait for execution command */
        while (true) { }
    }
}
