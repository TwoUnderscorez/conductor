package com.netflix.conductor.contribs.executionLimiting;

import java.util.Optional;

import com.google.inject.Inject;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.SemaphoreDAO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskDefAndWorkflowTaskLimitingDAO extends TaskDefLimitingDAO {
    public static final Logger logger = LoggerFactory.getLogger(TaskDefLimitingDAO.class);
    private SemaphoreDAO semaphoreDAO;
    private String semaphoreName;

    @Inject
    public TaskDefAndWorkflowTaskLimitingDAO(SemaphoreDAO semaphoreDAO, ExecutionDAO executionDAO) {
        super(executionDAO);
        this.semaphoreDAO = semaphoreDAO;
        semaphoreName = "limiting_semaphore.";
    }

    @Override
    public boolean exceedsInProgressLimit(Task task) {
        return !checkWorkflowTask(task) || super.exceedsInProgressLimit(task);
    }

    protected boolean checkWorkflowTask(Task task) {
        Optional<TaskDef> taskDef = task.getTaskDefinition();
        WorkflowTask wfTask = task.getWorkflowTask();
        int limit;
        if (wfTask.getGlobalConcurrentExecutionLimit() > 0) {
            limit = wfTask.getGlobalConcurrentExecutionLimit();
        } else {
            limit = wfTask.getLocalConcurrentExecutionLimit();
        }
        if (limit > 0 && taskDef.isPresent()) {
            TaskDef td = taskDef.get();
            return semaphoreDAO.tryAcquire(getLockOnName(task), task.getTaskId(), limit, td.getTimeoutSeconds() * 1000.0);
        }
        return true;
    }

    @Override
    public void notifyOfLimitedTaskCompletion(Task task) {
        WorkflowTask wfTask = task.getWorkflowTask();
        if(wfTask.getGlobalConcurrentExecutionLimit() > 0 || wfTask.getLocalConcurrentExecutionLimit() > 0)
            semaphoreDAO.release(getLockOnName(task), task.getTaskId());
    }

    private String getLockOnName(Task task) {
        WorkflowTask wfTask = task.getWorkflowTask();
        if (wfTask.getGlobalConcurrentExecutionLimit() > 0) {
            return semaphoreName + task.getReferenceTaskName();
        } else {
            return semaphoreName + task.getWorkflowType() + '_' + task.getReferenceTaskName();
        }
    }
}
