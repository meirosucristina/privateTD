package com.adobe.qe.toughday.internal.core.k8s;

/**
 * Describes all possible states in which an agent running in the cluster could be at a
 * certain moment.
 */
 public enum AgentStates {
    RUNNING,    /* the agent is executing tests from the test suite */
    WAITING_BALANCING_INSTRUCTIONS,     /* the agent is waiting to receive instruction from the driver */
    BALANCING   /* the agent is processing the balancing instructions received from the driver */
}