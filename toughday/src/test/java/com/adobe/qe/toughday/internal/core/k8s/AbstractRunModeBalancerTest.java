package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AbstractRunModeBalancerTest {

    @BeforeClass
    public static void onlyOnce() {
        System.setProperty("logFileName", ".");
        ((LoggerContext) LogManager.getContext(false)).reconfigure();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRunModeThrowsException() {
        RunMode dummyRunMode = new DummyRunMode();
        dummyRunMode.getRunModeBalancer().getRunModePropertiesToRedistribute(DummyRunMode.class, null);
    }

    @Test
    public void testAllPropertiesAreCollected() {
        DummyRunMode dummyRunMode = new DummyRunMode();
        Map<String, String> actual =
                dummyRunMode.getRunModeBalancer().getRunModePropertiesToRedistribute(DummyRunMode.class, dummyRunMode);

        Map<String, String> expected = new HashMap<>();
        expected.put("property1", "prop1");
        expected.put("property2", "prop2");

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNewValuesAreAssignedForAllRunModeProperties() {
        DummyRunMode dummyRunMode = new DummyRunMode();
        RebalanceInstructions rebalanceInstructions = new RebalanceInstructions();

        Map<String, String> runModeChanges = new HashMap<>();
        runModeChanges.put("property1", "prop1_new");
        runModeChanges.put("property2", "prop2_new");

        rebalanceInstructions.setRunModeProperties(runModeChanges);
        dummyRunMode.getRunModeBalancer().processRunModeInstructions(rebalanceInstructions, dummyRunMode);

        Assert.assertEquals("prop1_new", dummyRunMode.getProperty1());
        Assert.assertEquals("prop2_new", dummyRunMode.getProperty2());
        Assert.assertEquals("prop3", dummyRunMode.getProperty3());

    }

}
