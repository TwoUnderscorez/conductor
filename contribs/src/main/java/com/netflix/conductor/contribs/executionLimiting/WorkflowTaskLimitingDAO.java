package com.netflix.conductor.contribs.executionLimiting;

import com.google.inject.Inject;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.SemaphoreDAO;

public class WorkflowTaskLimitingDAO extends TaskDefAndWorkflowTaskLimitingDAO {

    @Inject
    public WorkflowTaskLimitingDAO(SemaphoreDAO semaphoreDAO, ExecutionDAO executionDAO) {
        super(semaphoreDAO, executionDAO);
    }
    
    @Override
    public boolean exceedsInProgressLimit(Task task) {
        return !checkWorkflowTask(task);
    }
}
