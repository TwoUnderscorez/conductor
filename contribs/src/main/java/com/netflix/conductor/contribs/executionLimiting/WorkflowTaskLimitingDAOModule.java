package com.netflix.conductor.contribs.executionLimiting;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.ConcurrentExecutionLimitingDAO;

public class WorkflowTaskLimitingDAOModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ConcurrentExecutionLimitingDAO.class).to(WorkflowTaskLimitingDAO.class);
    }
    
}
