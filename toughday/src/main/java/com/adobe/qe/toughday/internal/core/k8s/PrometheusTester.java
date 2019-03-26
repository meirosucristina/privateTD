package com.adobe.qe.toughday.internal.core.k8s;

import io.prometheus.client.*;
import io.prometheus.client.exporter.common.TextFormat;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Random;

import static spark.Spark.get;

public class PrometheusTester implements Runnable {
    /* Prometheus metrics registry for exposing metrics */
    final private CollectorRegistry registry = CollectorRegistry.defaultRegistry;

    // Simple test counter
    final private static Counter testCounter = Counter.build()
            .name("inc_counter").help("Amount of calls to /inc").register();

    // Test gauge for in-progress requests of various request types
    final private static Gauge inProgressRequests = Gauge.build()
            .name("in_progress_requests").help("In progress requests")
            .labelNames("method").register();

    // Test summary for average response latency
    final private static Summary avgRequestLatency = Summary.build()
            .name("request_latency").help("Average request latency").register();

    // Random instance for testing
    final Random random = new Random();

    public static void main(String[] args) {
        // Set up default Prometheus Java metrics (e.g. GC metrics)
        // DefaultExports.initialize();

        final PrometheusTester prometheusTester = new PrometheusTester();
        prometheusTester.run();
    }

    @Override
    public void run() {
        // Test routes to increment counter
        get("/counter", (req, res) -> {
            testCounter.inc();
            return "ok";
        });

        // Test in progress request metrics
        get("/gauge", (req, res) -> {
            // Increment in-progress GET requests
            try {
                inProgressRequests.labels("GET").inc();
                Thread.sleep(1000);
                return "ok";
            } finally {
                inProgressRequests.labels("GET").dec();
            }
        });

        // Test random request latency changes
        get("/summary", (req, res) -> {
            final Summary.Timer requestTimer = avgRequestLatency.startTimer();
            try {
                Thread.sleep(random.nextInt(5000));
                return "ok";
            } finally {
                requestTimer.observeDuration();
            }
        });

        // Monitoring & Health routes
        get("/metrics", this::exposePrometheusMetrics);
        get("/heatlhz", (req, res) -> "UP");
    }

    private String exposePrometheusMetrics(Request request, Response response) throws IOException {
        response.status(200);
        response.type(TextFormat.CONTENT_TYPE_004);
        final StringWriter writer = new StringWriter();
        TextFormat.write004(writer, registry.metricFamilySamples());
        return writer.toString();
    }
}