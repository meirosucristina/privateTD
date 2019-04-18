package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.config.ConfigParams;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.GenerateYamlConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;


/**
 * Class responsible for sending a request to the driver component in the K8s cluster
 * that will trigger the execution of TD.
 */
public class ExecutionTrigger {

    protected static final Logger LOG = LogManager.getLogger(ExecutionTrigger.class);

    private final Configuration configuration;
    private final String executionPath;

    public ExecutionTrigger(Configuration configuration) {
        this.configuration = configuration;
        // sanity check
        if (configuration.getK8SConfig().getDriverIp() == null || configuration.getK8SConfig().getDriverIp().isEmpty()) {
            throw new IllegalStateException("The public ip address at which the driver's service is accessible " +
                    " is required when running TD in distributed mode.");
        }

        this.executionPath = "http://" + configuration.getK8SConfig().getDriverIp() + ":80"  + "/config";
    }

    public void triggerExecution() {
        GenerateYamlConfiguration generateYaml = new GenerateYamlConfiguration(this.configuration.getConfigParams(), new HashMap<>());
        String yamlConfig = generateYaml.createYamlStringRepresentation();
        System.out.println(yamlConfig);
        HttpUtils httpUtils = new HttpUtils();

        if (!httpUtils.sendSyncHttpRequest(yamlConfig, executionPath, 3)) {
            LOG.warn("TD execution request could not be sent to driver. Make sure that driver is up" +
                    " and ready to process requests.");
        }
    }
}
