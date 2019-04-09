package com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes;

import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

public abstract class AbstractRunModeBalancer<T extends RunMode> implements RunModeBalancer<T> {
    @Override
    public void processRunModeInstructions(RebalanceInstructions rebalanceInstructions, T runMode) {
        Map<String, String> runModeProperties = rebalanceInstructions.getRunModeProperties();
        Arrays.stream(runMode.getClass().getDeclaredMethods())
                .filter(method -> runModeProperties.containsKey(Configuration.propertyFromMethod(method.getName())))
                .filter(method -> method.isAnnotationPresent(ConfigArgSet.class))
                .forEach(method -> {
                    String property = Configuration.propertyFromMethod(method.getName());

                    if (runModeProperties.containsKey(property)) {
                        System.out.println("[rebalace request] Setting property " + property + " to " + runModeProperties.get(property));

                        try {
                            method.invoke(runMode, runModeProperties.get(property));
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            e.printStackTrace();
                        }
                    }
                });
    }
}
