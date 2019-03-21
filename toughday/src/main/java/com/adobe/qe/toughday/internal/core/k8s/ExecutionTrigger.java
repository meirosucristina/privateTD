package com.adobe.qe.toughday.internal.core.k8s;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class ExecutionTrigger {

    private final static String URI = "http://driver-host/submitConfig";
    protected static final Logger LOG = LogManager.getLogger(ExecutionTrigger.class);

    private final String stringYamlConfig;


    public ExecutionTrigger(String stringYamlConfig) {
        if (stringYamlConfig == null || stringYamlConfig.isEmpty()) {
            throw new IllegalStateException("StringYamlConfiguration must not be null or empty.");
        }

        this.stringYamlConfig = stringYamlConfig;
    }

    public void triggerExecution() throws IOException {
        /* build HTTP query with yaml configuration as body */
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(URI);
        StringEntity params = new StringEntity(stringYamlConfig);

        request.setEntity(params);
        request.setHeader("Content-type", "text/plain");

        /* submit request and log response code */
        HttpResponse response = httpClient.execute(request);
        LOG.log(Level.INFO, "Driver response code: " + response.getStatusLine().getStatusCode());

        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
