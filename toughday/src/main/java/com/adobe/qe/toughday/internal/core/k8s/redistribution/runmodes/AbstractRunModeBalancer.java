package com.adobe.qe.toughday.internal.core.k8s.redistribution.runmodes;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.RunMode;
import com.adobe.qe.toughday.internal.core.k8s.redistribution.RebalanceInstructions;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractRunModeBalancer<T extends RunMode> implements RunModeBalancer<T> {

    @Override
    public Map<String, String> getRunModePropertiesToRedistribute(Class type, T object) {
        final Map<String, String> properties = new HashMap<>();

        if (object == null) {
            throw new IllegalArgumentException("Run mode object must not be null.");
        }

        Arrays.stream(type.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(ConfigArgGet.class))
                .filter(method -> method.getAnnotation(ConfigArgGet.class).redistribute())
                .forEach(method -> {
                    try {
                        String propertyName = Configuration.propertyFromMethod(method.getName());
                        Object value = method.invoke(object);

                        properties.put(propertyName, String.valueOf(value));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                });

        return properties;
    }

    @Override
    public void processRunModeInstructions(RebalanceInstructions rebalanceInstructions, T runMode) {
        if (rebalanceInstructions == null || runMode == null) {
            throw new IllegalArgumentException("Rebalance instructions and run mode must not be null.");
        }

        Map<String, String> runModeProperties = rebalanceInstructions.getRunModeProperties();
        if (runModeProperties == null || runModeProperties.isEmpty()) {
            return;
        }

        System.out.println("[AbstractRunModeBalancer] changing values for properties...");
        Arrays.stream(runMode.getClass().getDeclaredMethods())
                .filter(method -> runModeProperties.containsKey(Configuration.propertyFromMethod(method.getName())))
                .filter(method -> method.isAnnotationPresent(ConfigArgSet.class))
                .forEach(method -> {
                    String property = Configuration.propertyFromMethod(method.getName());

                    if (runModeProperties.containsKey(property)) {
                        System.out.println("[rebalance request] Setting property " + property + " to " + runModeProperties.get(property));

                        try {
                            method.invoke(runMode, runModeProperties.get(property));
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            e.printStackTrace();
                        }
                    } else {
                        System.out.println("key " + property + "not found");
                    }
                });
    }
}
