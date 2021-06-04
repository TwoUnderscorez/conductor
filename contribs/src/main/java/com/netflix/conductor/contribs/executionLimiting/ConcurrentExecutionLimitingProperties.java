package com.netflix.conductor.contribs.executionLimiting;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("conductor.concurrent-execution-limiting-dao")
public class ConcurrentExecutionLimitingProperties {
    private String implementation = "taskdef";

    public String getImplementation() {
        return implementation;
    }

    public void setImplementation(String implementation) {
        this.implementation = implementation;
    }
}
