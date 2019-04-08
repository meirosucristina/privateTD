package com.adobe.qe.toughday.internal.core.k8s;

import java.util.Map;

/** Contains all the information needed by the agents for updating their configuration when
 * the work needs to be rebalanced.
 */
public class RebalanceInstructions {
    private Map<String, Long> counts;
    private Map<String, String> runModeProperties;

    // dummy constructor, required for Jackson
    public RebalanceInstructions() {

    }

    public RebalanceInstructions(Map<String, Long> counts, Map<String, String> runModeProperties) {
        this.counts = counts;
        this.runModeProperties = runModeProperties;
    }

    // public getters are required by Jackson
    public Map<String, Long> getCounts() {
        return this.counts;
    }

    public void setCounts(Map<String, Long> counts) {
        this.counts = counts;
    }

    public Map<String, String> getRunModeProperties() { return this.runModeProperties; }

    public void setRunModeProperties(Map<String, String> runModeProperties) {
        this.runModeProperties = runModeProperties;
    }
}