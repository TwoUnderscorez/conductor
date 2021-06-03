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
package com.netflix.conductor.redis.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.dao.SemaphoreDAO;
import com.netflix.conductor.redis.config.RedisProperties;
import com.netflix.conductor.redis.jedis.JedisProxy;

import javax.inject.Inject;
import javax.inject.Singleton;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Trace
public class RedisSemaphoreDAO extends BaseDynoDAO implements SemaphoreDAO {

    private static final Logger logger = LoggerFactory.getLogger(RedisRateLimitingDAO.class);

    private JedisProxy dynoClient;

    @Inject
    protected RedisSemaphoreDAO(JedisProxy dynoClient, ObjectMapper objectMapper, ConductorProperties cProps, RedisProperties rProps) {
        super(dynoClient, objectMapper, cProps, rProps);
        this.dynoClient = dynoClient;
    }

    @Override
    public boolean tryAcquire(String semaphoreName, String identifier, int limit, double timeoutMillis) {
        double now = Instant.now().getMillis() / 1000.0;
        dynoClient.zremrangeByScore(semaphoreName, "-inf", String.valueOf(now - timeoutMillis));
        dynoClient.zadd(semaphoreName, now, identifier);
        if (dynoClient.zrank(semaphoreName, identifier) < limit)
            return true;
        dynoClient.zrem(semaphoreName, identifier);
        return false;
    }

    @Override
    public boolean release(String semaphoreName, String identifier) {
        return dynoClient.zrem(semaphoreName, identifier) > 0;
    }

}