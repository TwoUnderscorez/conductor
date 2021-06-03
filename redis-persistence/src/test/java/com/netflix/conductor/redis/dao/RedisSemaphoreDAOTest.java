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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.dao.SemaphoreDAO;
import com.netflix.conductor.redis.config.RedisProperties;
import com.netflix.conductor.redis.jedis.JedisProxy;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.redis.jedis.JedisMock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import redis.clients.jedis.commands.JedisCommands;

@ContextConfiguration(classes = {TestObjectMapperConfiguration.class})
@RunWith(SpringRunner.class)
public class RedisSemaphoreDAOTest {

	@Autowired
    private ObjectMapper objectMapper;
	
	private SemaphoreDAO semaphoreDAO;

	@Before
	public void init() {
		ConductorProperties conductorProperties = mock(ConductorProperties.class);
        RedisProperties redisProperties = mock(RedisProperties.class);
        when(redisProperties.getEventExecutionPersistenceTTL()).thenReturn(Duration.ofSeconds(5));
        JedisCommands jedisMock = new JedisMock();
        JedisProxy jedisProxy = new JedisProxy(jedisMock);

		semaphoreDAO = new RedisSemaphoreDAO(jedisProxy, objectMapper, conductorProperties, redisProperties);
	}

	@Rule
	public ExpectedException expected = ExpectedException.none();

	@Test
	public void test() {
		String semaphore_name = "test_semaphore";
		double timeout = 10000.0;
		int limit = 5;
		for (int i = 1; i < 6; i++) {
			assertTrue(
				String.format("Initial try acquire %d/5", i),
				semaphoreDAO.tryAcquire(semaphore_name, String.valueOf(i), limit, timeout)
			);
		}
		assertFalse(
			"Try to acquire over the allowed limit", 
			semaphoreDAO.tryAcquire(semaphore_name, "identifier", limit, timeout)
		);
		assertTrue(
			"Release No3", 
			semaphoreDAO.release(semaphore_name, "3")
		);
		assertTrue(
			"Acquire No6",
			semaphoreDAO.tryAcquire(semaphore_name, "6", limit, timeout)
		);
		for (int i = 1; i < 7; i++) {
			assertTrue(
				String.format("Final release %d/6", i),
				semaphoreDAO.release(semaphore_name, String.valueOf(i)) || i == 3 
			);
		}
	}
}