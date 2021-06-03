package com.netflix.conductor.mysql.dao;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.dao.SemaphoreDAO;
import com.netflix.conductor.mysql.util.MySQLDAOTestUtil;
import org.springframework.beans.factory.annotation.Autowired;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

@ContextConfiguration(classes = {TestObjectMapperConfiguration.class})
@RunWith(SpringRunner.class)
public class MySQLSemaphoreDAOTest {
    
    private MySQLDAOTestUtil testUtil;
    private SemaphoreDAO semaphoreDAO;

    @Autowired
    private ObjectMapper objectMapper;

    @Rule
    public TestName name = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public MySQLContainer<?> mySQLContainer;

    @Before
    public void setup() {
        mySQLContainer = new MySQLContainer<>(DockerImageName.parse("mysql")).withDatabaseName(name.getMethodName());
        mySQLContainer.start();
        testUtil = new MySQLDAOTestUtil(mySQLContainer, objectMapper, name.getMethodName());
        semaphoreDAO = new MySQLSemaphoreDAO(testUtil.getObjectMapper(), testUtil.getDataSource());
    }

    @After
    public void teardown() {
        testUtil.resetAllData();
        testUtil.getDataSource().close();
    }
    
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
