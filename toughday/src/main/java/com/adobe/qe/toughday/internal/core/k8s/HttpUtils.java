package com.adobe.qe.toughday.internal.core.k8s;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Future;

public class HttpUtils {

    public static final String URL_PREFIX = "http://";
    public static final String EXECUTION_PATH = "/config";
    public static final String HEARTBEAT_PATH = "/heartbeat";
    public static final String SUBMIT_TASK_PATH = "/submitTask";
    public static final String AGENT_PREFIX_NAME = "Agent";
    public static final String REBALANCE_PATH = "/rebalance";


    public Future<HttpResponse> sendAsyncHttpRequest(String URI, String content,
                                                     CloseableHttpAsyncClient asyncClient) {
        HttpPost taskRequest = new HttpPost(URI);
        try {
            StringEntity params = new StringEntity(content);
            taskRequest.setEntity(params);
            taskRequest.setHeader("Content-type", "text/plain");

            return asyncClient.execute(taskRequest, null);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void sendSyncHttpRequest(String requestContent, String agentURI) {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(agentURI);

        try {
            StringEntity params = new StringEntity(requestContent);
            request.setEntity(params);
            request.setHeader("Content-type", "text/plain");

            // submit request and wait for ack from agent
            HttpResponse response = httpClient.execute(request);
            System.out.println("Response code is " + response.getStatusLine().getStatusCode());

        } catch (IOException e)  {
            System.out.println("Http request could not be sent to  " + agentURI);
            System.out.println(e.getMessage());
        }
    }

}
