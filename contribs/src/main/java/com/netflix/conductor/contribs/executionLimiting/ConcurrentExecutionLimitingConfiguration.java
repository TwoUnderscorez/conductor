package com.netflix.conductor.contribs.executionLimiting;

import com.netflix.conductor.dao.ConcurrentExecutionLimitingDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.SemaphoreDAO;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ConcurrentExecutionLimitingProperties.class)
public class ConcurrentExecutionLimitingConfiguration {
    @Bean
    public ConcurrentExecutionLimitingDAO getConcurrentExecutionLimitingDAO(
        ConcurrentExecutionLimitingProperties properties, 
        ExecutionDAO executionDAO, 
        SemaphoreDAO semaphoreDAO) {
        switch (properties.getImplementation()) {
            case "taskdef_and_workflowtask":
                return new TaskDefAndWorkflowTaskLimitingDAO(semaphoreDAO, executionDAO);
            case "workflowtask":
                return new WorkflowTaskLimitingDAO(semaphoreDAO, executionDAO);
            case "taskdef":
            default:
                return new TaskDefLimitingDAO(executionDAO);
        }
    }
}
