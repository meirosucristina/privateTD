package com.adobe.qe.toughday.internal.core.k8s;

import static spark.Spark.*;

/**
 * Driver component for K8s cluster.
 */
public class Driver {

    public void run() {

        /* expose http endpoint for registering new agents to the cluster */
        get("/registerAgent/:hostname", (request, response) -> {
            /* log some messages to display in the driver component */
            System.out.println("Agent " + request.params(":hostname") + " has been registered.");
            // Show something
            return request.params(":hostname");
        });


        /* loop forever */
        while(true) {

        }
    }
}
