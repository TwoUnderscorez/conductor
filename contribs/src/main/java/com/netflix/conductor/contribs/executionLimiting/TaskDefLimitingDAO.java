package com.netflix.conductor.contribs.executionLimiting;

import java.util.List;
import java.util.Optional;

import com.google.inject.Inject;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.dao.ConcurrentExecutionLimitingDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.metrics.Monitors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskDefLimitingDAO implements ConcurrentExecutionLimitingDAO {
	public static final Logger logger = LoggerFactory.getLogger(TaskDefLimitingDAO.class);

    private ExecutionDAO executionDAO;

    @Inject
    public TaskDefLimitingDAO(ExecutionDAO executionDAO) {
        this.executionDAO = executionDAO;
    }

    @Override
    public boolean exceedsInProgressLimit(Task task) {
        Optional<TaskDef> taskDefinition = task.getTaskDefinition();
		if(!taskDefinition.isPresent()) {
			return false;
		}
		int limit = taskDefinition.get().concurrencyLimit();
		if(limit <= 0) {
			return false;
        }
        
        long current = getInProgressTaskCount(task.getTaskDefName());
		if(current >= limit) {
			logger.info("Task execution count limited. task - {}:{}, limit: {}, current: {}", task.getTaskId(), task.getTaskDefName(), limit, current);
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
            System.out.println("current >= limit");
			return true;
        }

        String taskId = task.getTaskId();
        task.getTaskType();
        List<String> tasksInProgressInOrderOfArrival = executionDAO.findAllTasksInProgressInOrderOfArrival(task, limit);
        if(current < 0) current = tasksInProgressInOrderOfArrival.size();

        boolean rateLimited = !tasksInProgressInOrderOfArrival.contains(taskId) && current >= limit;

        if (rateLimited) {
            System.out.println("!tasksInProgressInOrderOfArrival.contains(taskId)");
            logger.info("Task execution count limited. {}:{}, limit {}, current {}", task.getTaskId(), task.getTaskDefName(), limit, current);
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
        }

        return rateLimited;
    }

    private long getInProgressTaskCount(String taskDefName) {
        try {
            return executionDAO.getInProgressTaskCount(taskDefName);
        } catch (UnsupportedOperationException e) {
            return -1;
        }
    }
    
}
