/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.conductor.dao;

import com.netflix.conductor.common.metadata.tasks.Task;

public interface ConcurrentExecutionLimitingDAO {
    
    /**
     * Checks if the Task is execution limited or not
     * @param task: which needs to be evaluated whether it is execution limited or not
     * @return true: If the {@link Task} is execution limited
     * 		false: If the {@link Task} is not execution limited
     */
    boolean exceedsInProgressLimit(Task task);

    /**
     * If {@link ConcurrentExecutionLimitingDAO.exceedsInProgressLimit} returned false,
     * this should be called upon task completion.
     * @param task The completed task
     */
    default void notifyOfLimitedTaskCompletion(Task task) {};
}
