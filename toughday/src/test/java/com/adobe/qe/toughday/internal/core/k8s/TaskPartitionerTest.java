package com.adobe.qe.toughday.internal.core.k8s;

import com.adobe.qe.toughday.MockTest;
import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.internal.core.ReflectionsContainer;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.engine.runmodes.ConstantLoad;
import com.adobe.qe.toughday.internal.core.engine.runmodes.Normal;
import com.adobe.qe.toughday.metrics.Metric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.*;

import java.util.*;
import java.util.stream.Collectors;

public class TaskPartitionerTest {
    private final TaskPartitioner taskPartitioner = new TaskPartitioner();
    private List<String> cmdLineArgs;
    private static ReflectionsContainer reflections = ReflectionsContainer.getInstance();


    @BeforeClass
    public static void onlyOnce() {
        System.setProperty("logFileName", ".");

        reflections.getTestClasses().put("MockTest", MockTest.class);
        ((LoggerContext) LogManager.getContext(false)).reconfigure();
    }


    @Before
    public void before() {
        cmdLineArgs = new ArrayList<>(Collections.singletonList("--host=localhost"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPhaseThrowsException() throws CloneNotSupportedException {
        taskPartitioner.splitPhase(null, new ArrayList<>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullListOfAgentsThrowsException() throws CloneNotSupportedException {
        taskPartitioner.splitPhase(new Phase(), null);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoAvailableAgentsThrowsException() throws CloneNotSupportedException {
        taskPartitioner.splitPhase(new Phase(), new ArrayList<>());
    }

    @Test
    public void testNumberOfTasksIsEqualToTheNumberOfAgents() throws Exception {
        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2");
        configuration.getPhases().forEach(phase -> {
            try {
                Map<String, Phase> taskMap = taskPartitioner.splitPhase(phase, mockAgents);
                Assert.assertEquals(mockAgents.size(), taskMap.keySet().size());
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testConcurrencyDistribution() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--runmode", "type=normal", "concurrency=320"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2", "Agent3");
        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));

        Map<String, Phase> taskMap = taskPartitioner.splitPhase(configuration.getPhases().get(0), mockAgents);
        Assert.assertEquals(108, ((Normal) taskMap.get("Agent1").getRunMode()).getConcurrency());
        Assert.assertEquals(106, ((Normal) taskMap.get("Agent2").getRunMode()).getConcurrency());
        Assert.assertEquals(106, ((Normal) taskMap.get("Agent3").getRunMode()).getConcurrency());
    }

    @Test
    public void testLoadDistribution() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--runmode", "type=constantload", "load=182"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2", "Agent3");
        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));

        Map<String, Phase> taskMap = taskPartitioner.splitPhase(configuration.getPhases().get(0), mockAgents);
        Assert.assertEquals(62, ((ConstantLoad) taskMap.get("Agent1").getRunMode()).getLoad());
        Assert.assertEquals(60, ((ConstantLoad) taskMap.get("Agent2").getRunMode()).getLoad());
        Assert.assertEquals(60, ((ConstantLoad) taskMap.get("Agent3").getRunMode()).getLoad());
    }

    @Test
    public void testVariableConcurrencyDistribution() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--runmode", "type=normal", "start=3", "end=12", "rate=3"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2");

        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));
        Map<String, Phase> taskMap = taskPartitioner.splitPhase(configuration.getPhases().get(0), mockAgents);
        Normal initialRunMode = (Normal) configuration.getRunMode();

        Normal firstAgentRunMode = (Normal) taskMap.get("Agent1").getRunMode();
        Assert.assertEquals(2, firstAgentRunMode.getStart());
        Assert.assertEquals(6, firstAgentRunMode.getEnd());
        Assert.assertEquals(2, firstAgentRunMode.getRate());
        // check that the interval property does not change
        Assert.assertEquals(initialRunMode.getInterval(), firstAgentRunMode.getInterval());

        Normal secondAgentRunMode = (Normal) taskMap.get("Agent2").getRunMode();
        Assert.assertEquals(1, secondAgentRunMode.getStart());
        Assert.assertEquals(6, secondAgentRunMode.getEnd());
        Assert.assertEquals(1, secondAgentRunMode.getRate());
        // check that the interval property does not change
        Assert.assertEquals(initialRunMode.getInterval(), secondAgentRunMode.getInterval());
    }

    @Test
    public void testVariableConcurrencyWithRateLowerThanNumberOfAgents() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--runmode", "type=normal", "start=3", "end=9", "interval=1s", "rate=1"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2");

        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));
        Map<String, Phase> taskMap = taskPartitioner.splitPhase(configuration.getPhases().get(0), mockAgents);
        Normal initialRunMode = (Normal) configuration.getRunMode();

        Normal firstAgentRunMode = (Normal) taskMap.get("Agent1").getRunMode();
        Assert.assertEquals(3, firstAgentRunMode.getStart());
        Assert.assertEquals(6, firstAgentRunMode.getEnd());
        Assert.assertEquals("2s", firstAgentRunMode.getInterval());
        Assert.assertEquals(0, firstAgentRunMode.getInitialDelay());

        Normal secondAgentRunMode = (Normal) taskMap.get("Agent2").getRunMode();
        Assert.assertEquals(0, secondAgentRunMode.getStart());
        Assert.assertEquals(3, secondAgentRunMode.getEnd());
        Assert.assertEquals("2s", secondAgentRunMode.getInterval());
        Assert.assertEquals(1000, secondAgentRunMode.getInitialDelay());

        // check that the initial rate is not modified
        Assert.assertEquals(initialRunMode.getRate(), firstAgentRunMode.getRate());
        Assert.assertEquals(initialRunMode.getRate(), secondAgentRunMode.getRate());
    }

    @Test
    public void testVariableLoadDistribution() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--runmode", "type=constantload", "start=3", "end=12", "rate=3"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2");

        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));
        Map<String, Phase> taskMap = taskPartitioner.splitPhase(configuration.getPhases().get(0), mockAgents);

        ConstantLoad firstAgentRunMode = (ConstantLoad) taskMap.get("Agent1").getRunMode();
        Assert.assertEquals(2, firstAgentRunMode.getStart());
        Assert.assertEquals(6, firstAgentRunMode.getEnd());
        Assert.assertEquals(2, firstAgentRunMode.getRate());

        ConstantLoad secondAgentRunMode = (ConstantLoad) taskMap.get("Agent2").getRunMode();
        Assert.assertEquals(1, secondAgentRunMode.getStart());
        Assert.assertEquals(6, secondAgentRunMode.getEnd());
        Assert.assertEquals(1, secondAgentRunMode.getRate());
    }

    @Test
    public void testVariableLoadWithRateLowerThanTheNumberOfAgents() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--runmode", "type=constantload", "start=10", "end=20", "rate=2", "interval=2s"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2", "Agent3");

        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));
        Map<String, Phase> taskMap = taskPartitioner.splitPhase(configuration.getPhases().get(0), mockAgents);
        ConstantLoad initialRunMode = (ConstantLoad) configuration.getPhases().get(0).getRunMode();

        taskMap.forEach((key, value) -> {
            ConstantLoad taskRunMode = (ConstantLoad) value.getRunMode();
            Assert.assertEquals(initialRunMode.getEnd(), taskRunMode.getEnd());
            Assert.assertEquals(initialRunMode.getRate(), taskRunMode.getOneAgentRate());
            Assert.assertEquals(6, taskRunMode.getRate());
            Assert.assertEquals("6s", taskRunMode.getInterval());

        });

        /* the values for rate property are checked for the first complete cycle
        (nr_agents * interval period of time) */
        ConstantLoad firstAgentRunMode = (ConstantLoad) taskMap.get("Agent1").getRunMode();
        Assert.assertEquals(10, firstAgentRunMode.getStart());
        Assert.assertEquals(0, firstAgentRunMode.getInitialDelay());

        ConstantLoad secondAgentRunMode = (ConstantLoad) taskMap.get("Agent2").getRunMode();
        Assert.assertEquals(12, secondAgentRunMode.getStart());
        Assert.assertEquals(2000, secondAgentRunMode.getInitialDelay());

        ConstantLoad thirdAgentRunMode = (ConstantLoad) taskMap.get("Agent3").getRunMode();
        Assert.assertEquals(14, thirdAgentRunMode.getStart());
        Assert.assertEquals(4000, thirdAgentRunMode.getInitialDelay());

    }

    @Test
    public void testWaitTimeIsNotModified() throws Exception {
        Configuration configuration = new Configuration(cmdLineArgs.toArray(new String[0]));
        Normal initialRunMode = (Normal) configuration.getPhases().get(0).getRunMode();
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2", "Agent3");

        Map<String, Phase> taskMap = taskPartitioner.splitPhase(configuration.getPhases().get(0), mockAgents);
        taskMap.forEach((key, value) ->
                Assert.assertEquals(initialRunMode.getWaitTime(), ((Normal) value.getRunMode()).getWaitTime()));
    }

    @Test
    public void testCountIsDistributedForTestsInTestSuite() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--add", "MockTest", "name=Test1", "count=201"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2");

        Phase phase = new Configuration(cmdLineArgs.toArray(new String[0])).getPhases().get(0);
        Map<String, Phase> taskMap = taskPartitioner.splitPhase(phase, mockAgents);

        taskMap.get("Agent1").getTestSuite().getTests().forEach(test -> {
            Assert.assertEquals(101, test.getCount());
        });

        taskMap.get("Agent2").getTestSuite().getTests().forEach(test -> {
            Assert.assertEquals(100, test.getCount());
        });
    }

    @Test
    public void testEachAgentRunsTheCompleteTestSuite() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--add", "MockTest", "name=Test1", "--add", "MockTest", "name=Test2"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2", "Agent3");

        Phase phase = new Configuration(cmdLineArgs.toArray(new String[0])).getPhases().get(0);
        Set<String> testNames = phase.getTestSuite().getTests().stream()
                .map(AbstractTest::getName)
                .collect(Collectors.toSet());

        Map<String, Phase> taskMap = taskPartitioner.splitPhase(phase, mockAgents);
        taskMap.forEach((key, value) -> {
            Set<String> namesDiff = value.getTestSuite().getTests().stream()
                    .map(AbstractTest::getName)
                    .collect(Collectors.toSet());
            namesDiff.removeAll(testNames);

            Assert.assertEquals(0, namesDiff.size());
        });
    }

    @Test
    public void testEachAgentHasAllMetrics() throws Exception {
        cmdLineArgs.addAll(Arrays.asList("--add", "Passed", "--add", "Failed", "--add", "Percentile", "value=90"));
        List<String> mockAgents = Arrays.asList("Agent1", "Agent2");

        Phase phase = new Configuration(cmdLineArgs.toArray(new String[0])).getPhases().get(0);
        Set<String> metricNames = phase.getMetrics().stream().map(Metric::getName).collect(Collectors.toSet());

        Map<String, Phase> taskMap = taskPartitioner.splitPhase(phase, mockAgents);
        taskMap.forEach((key, value) -> {
            Set<String> phaseMetricNames = phase.getMetrics().stream().map(Metric::getName).collect(Collectors.toSet());
            phaseMetricNames.removeAll(metricNames);

            Assert.assertEquals(0, phaseMetricNames.size());
        });
    }

}
