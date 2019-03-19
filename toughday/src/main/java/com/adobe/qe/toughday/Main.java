/*
Copyright 2015 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/
package com.adobe.qe.toughday;

import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.config.parsers.cli.CliParser;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.k8s.Agent;
import com.adobe.qe.toughday.internal.core.k8s.Driver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static spark.Spark.*;

/**
 * Main class. Creates a Configuration and an engine and runs the tests.
 */
public class Main {
    private static final Logger LOG;

    static {
        System.setProperty("logFileName", ".");
        LOG = LogManager.getLogger(Main.class);
    }

    public static void main(String[] args) {

        // path for health check(used as liveness probe for Kubernetes pod)
        get("/healthz", ((request, response) -> "Healthy"));
        CliParser cliParser = new CliParser();
        System.out.println();

        try {
            Configuration configuration = null;
            try {
                configuration = new Configuration(args);
                if (cliParser.printHelp(args)) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                LOG.error("Bad configuration: {}", e.getMessage());
                System.out.println();
                System.out.println();
                cliParser.printShortHelp();
                System.exit(1);
            }

            /* check if TD runs as an Agent in K8s */
            if (configuration.getGlobalArgs().getK8sAgent()) {
                Agent agent = new Agent();
                agent.start();
                System.exit(0);
            } else if (configuration.getGlobalArgs().getDriverMode()) {
                /* TD runs in driver mode */
                Driver driver = new Driver();
                driver.run();
                System.exit(0);
            }

            Engine engine = new Engine(configuration);
            engine.runTests();

            System.exit(0);
        } catch (Throwable t) {
            LOG.error("Error encountered: "
                    + (t.getMessage() != null ? t.getMessage() : "Please check toughday.log for more information."));
            LogManager.getLogger(Engine.class).error("Error encountered", t);
        }
        System.exit(0);
    }
}
