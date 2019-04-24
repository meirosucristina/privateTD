package com.adobe.qe.toughday.internal.core.distributedtd;

import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;

public class HttpUtils {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    public static final String URL_PREFIX = "http://";

    public boolean sendSyncHttpRequest(String requestContent, String URI) {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(URI);

        try {
            StringEntity params = new StringEntity(requestContent);
            request.setEntity(params);
            request.setHeader("Content-type", "text/plain");

            // submit request and wait for ack from agent
            HttpResponse response = httpClient.execute(request);
            return checkSuccessfulRequest(response.getStatusLine().getStatusCode());

        } catch (IOException e)  {
            LOG.warn("Http request could not be sent to  " + URI + ". Received error " + e.getMessage());
        }

        return false;
    }

    public HttpResponse sendHeartbeatRequest(String agentURI, int retries){
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
        while (retries > 0) {
            try {
                agentResponse = heartBeatHttpClient.execute(heartbeatRequest);
            } catch (IOException e) {
                retries--;
                // maybe log warning to indicate why heartbeat failed
                continue;
            }

            if (agentResponse != null) {
                successfulRequest = checkSuccessfulRequest(agentResponse.getStatusLine().getStatusCode());
                if (successfulRequest) {
                    return agentResponse;
                }
            }

            retries--;
        }

        return null;
    }

    private boolean checkSuccessfulRequest(int responseCode) {
        return responseCode >= 200 && responseCode < 300;
    }

    public boolean sendSyncHttpRequest(String requestContent, String URI, int retries) {
        boolean successfulRequest = false;

        while (retries > 0 && !successfulRequest) {
            successfulRequest = sendSyncHttpRequest(requestContent, URI);
            retries--;
        }

        return successfulRequest;
    }

}
