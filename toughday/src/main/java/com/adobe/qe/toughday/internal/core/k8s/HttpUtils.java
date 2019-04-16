package com.adobe.qe.toughday.internal.core.k8s;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
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

    public boolean sendSyncHttpRequest(String requestContent, String URI) {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(URI);

        try {
            StringEntity params = new StringEntity(requestContent);
            request.setEntity(params);
            request.setHeader("Content-type", "text/plain");

            // submit request and wait for ack from agent
            HttpResponse response = httpClient.execute(request);
            System.out.println("Response code is " + response.getStatusLine().getStatusCode());
            return checkSuccessfulRequest(response.getStatusLine().getStatusCode());

        } catch (IOException e)  {
            System.out.println("Http request could not be sent to  " + URI);
            System.out.println(e.getMessage());
        }

        return false;
    }

    public HttpResponse sendHeartbeatRequest(String agentURI, int retrial ){
        CloseableHttpClient heartBeatHttpClient = HttpClientBuilder.create().build();
        HttpResponse agentResponse;

        // configure timeout limits
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(1000)
                .setConnectTimeout(1000)
                .setSocketTimeout(1000)
                .build();
        HttpGet heartbeatRequest = new HttpGet(agentURI);
        heartbeatRequest.setConfig(requestConfig);

        boolean successfulRequest;
        while (retrial > 0) {
            try {
                agentResponse = heartBeatHttpClient.execute(heartbeatRequest);
            } catch (IOException e) {
                retrial--;
                // maybe log warning to indicate why heartbeat failed
                continue;
            }

            if (agentResponse != null) {
                successfulRequest = checkSuccessfulRequest(agentResponse.getStatusLine().getStatusCode());
                if (successfulRequest) {
                    return agentResponse;
                }
            }

            retrial--;
        }

        return null;
    }

    private boolean checkSuccessfulRequest(int responseCode) {
        return responseCode >= 200 && responseCode < 300;
    }

    public boolean sendSyncHttpRequest(String requestContent, String URI, int retrial) {
        boolean successfulRequest = false;

        while (retrial > 0 && !successfulRequest) {
            successfulRequest = sendSyncHttpRequest(requestContent, URI);
            retrial--;
        }

        return successfulRequest;
    }

}
