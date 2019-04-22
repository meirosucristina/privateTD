package com.adobe.qe.toughday.publishers;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.api.core.MetricResult;
import com.adobe.qe.toughday.api.core.Publisher;
import com.adobe.qe.toughday.api.core.benchmark.TestResult;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringEscapeUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Publisher used by the agents in the cluster to publish their individual results. Aggregated
 * metrics could then by determined using tools like Grafana.
 */
public class InfluxDbPublisher extends Publisher {
    private static final String DEFAULT_DATABASE_NAME = "td-on-distributedtd";
    private static final String DEFAULT_PORT = "8086";
    private static final String DEFAULT_TABLE_NAME = "testResults";
    private static final String DEFAULT_CLUSTER_NAMESPACE = "default";

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbPublisher.class);

    private String influxName;
    private int port = Integer.parseInt(DEFAULT_PORT);
    private String databaseName = DEFAULT_DATABASE_NAME;
    private String tableName = DEFAULT_TABLE_NAME;
    private String clusterNamespace = DEFAULT_CLUSTER_NAMESPACE;
    private InfluxDB influxDB;
    private boolean setup = false;
    private final Gson GSON = new Gson();


    @ConfigArgSet(required = false, defaultValue = "8086", desc = "Port used for connecting to the Influx database.")
    public void setPort(String port) {
        if (!port.matches("^[0-9]+$")) {
            throw new IllegalArgumentException("Port must be a number");
        }

        this.port = Integer.parseInt(port);
    }

    @ConfigArgGet
    public int getPort() {
        return this.port;
    }

    @ConfigArgSet(desc = "Influx service name")
    public void setInfluxName(String influxName) {
        this.influxName = influxName;
    }

    @ConfigArgGet
    public String getInfluxName() {
        return this.influxName;
    }

    @ConfigArgSet(required = false, defaultValue = "td-on-distributedtd", desc = "Name of the database in which" +
            "the results are stored.")
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @ConfigArgGet
    public String getDatabaseName() {
        return this.databaseName;
    }

    @ConfigArgSet(required = false, defaultValue = DEFAULT_TABLE_NAME, desc = "The name of the table" +
            " in which the results are stored.")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @ConfigArgGet
    public String getTableName() {
        return this.tableName;
    }

    @ConfigArgSet(required = false, defaultValue = DEFAULT_CLUSTER_NAMESPACE, desc = "The namespace in the cluster" +
            " where the InfluxDb was deployed.")
    public void setClusterNamespace(String clusterNamespace) {
        this.clusterNamespace = clusterNamespace;
    }

    @ConfigArgGet
    public String getClusterNamespace() {
        return this.clusterNamespace;
    }

    private void verifyConnection() {
        Pong response = this.influxDB.ping();
        if (response.getVersion().equalsIgnoreCase("unknown")) {
            LOG.warn("Unable to ping database. Connection was lost.");
        }

        // TODO: try reestablishing the connection. Log error in case of multiple consecutive failures
    }

    private void setup() {
        String connectionString = "http://" + this.influxName + "." + this.clusterNamespace + ":" + this.port;

        this.influxDB = InfluxDBFactory.connect(connectionString);
        // avoid log messages at the console
        this.influxDB.setLogLevel(InfluxDB.LogLevel.NONE);

        verifyConnection();
        createDatabase();

        this.setup = true;
    }

    private void createDatabase() {
        this.influxDB.createDatabase(this.databaseName);
    }


    @Override
    protected void doPublishAggregatedIntermediate(Map<String, List<MetricResult>> results) {
        // not supported by this kind of publisher
    }

    @Override
    protected void doPublishAggregatedFinal(Map<String, List<MetricResult>> results) {
        // not supported by this kind of publisher
    }

    @Override
    protected void doPublishRaw(Collection<TestResult> testResults) {
        if (!setup) {
            setup();
        }

        BatchPoints batchPoints = BatchPoints.database(this.databaseName)
                .retentionPolicy("autogen")
                .tag("async", "true")
                .build();

        testResults.forEach(testResult -> {
            Object data = testResult.getData();

            Point point = Point.measurement("testResults")
                    .addField("name", testResult.getTestFullName())
                    .addField("status", testResult.getStatus().toString())
                    .addField("thread", testResult.getThreadId())
                    .addField("start timestamp", testResult.getFormattedStartTimestamp())
                    .addField("end timestamp", testResult.getFormattedEndTimestamp())
                    .addField("duration", testResult.getDuration())
                    .addField("data", StringEscapeUtils.escapeCsv(data != null ? GSON.toJson(data) : ""))
                    .build();
            batchPoints.point(point);
        });

        //TODO: investigate why data field is not printed

        this.influxDB.write(batchPoints);
    }

    @Override
    public void finish() {
        this.influxDB.close();
    }

}
