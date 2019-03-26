package com.adobe.qe.toughday.publishers;

import com.adobe.qe.toughday.api.core.MetricResult;
import com.adobe.qe.toughday.api.core.Publisher;
import com.adobe.qe.toughday.api.core.benchmark.TestResult;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import spark.Request;
import spark.Response;
import spark.Spark;


import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static spark.Spark.get;

public class PrometheusPublisher extends Publisher {
    /* Prometheus metrics registry for exposing metrics */
    final private CollectorRegistry registry = CollectorRegistry.defaultRegistry;

    @Override
    protected void doPublishAggregatedIntermediate(Map<String, List<MetricResult>> results) {
        // metrics are automatically pulled by prometheus service
    }

    @Override
    protected void doPublishAggregatedFinal(Map<String, List<MetricResult>> results) {
        // metrics are automatically pulled by prometheus service
    }

    @Override
    protected void doPublishRaw(Collection<TestResult> testResults) {
        throw new IllegalStateException("Raw publishing is not supported for Prometheus publisher.");
    }

    @Override
    public void finish() {
    }

    private String exposePrometheusMetrics(Request request, Response response) throws IOException {
        response.status(200);
        response.type(TextFormat.CONTENT_TYPE_004);
        final StringWriter writer = new StringWriter();
        TextFormat.write004(writer, registry.metricFamilySamples());
        return writer.toString();
    }
}
