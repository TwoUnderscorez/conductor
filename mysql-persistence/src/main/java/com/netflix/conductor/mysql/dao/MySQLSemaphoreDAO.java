package com.netflix.conductor.mysql.dao;

import java.sql.Connection;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.dao.SemaphoreDAO;

public class MySQLSemaphoreDAO extends MySQLBaseDAO implements SemaphoreDAO {

    public MySQLSemaphoreDAO(ObjectMapper om, DataSource dataSource) {
        super(om, dataSource);
    }

    @Override
    public boolean tryAcquire(String semaphoreName, String identifier, int limit, double timeoutSeconds) {
        // logger.error("tryAcquire %s, %s, %d, %f", semaphoreName, identifier, limit, timeoutMillis);
        // Delete timed out semaphores
        withTransaction(tx -> deleteExpired(tx, semaphoreName, (int)(timeoutSeconds * 1_000_000)));
        // Insert ourselves to the semaphore
        withTransaction(tx -> insertRecord(tx, semaphoreName, identifier));
        // Return true if we have the semaphore, false otherwise
        return deleteRecordIfExceedsLimit(semaphoreName, identifier, limit);
    }

    @Override
    public boolean release(String semaphoreName, String identifier) {
        // logger.error("release %s, %s", semaphoreName, identifier);
        withTransaction(tx -> deleteRecord(tx, semaphoreName, identifier));
        return true;
    }

    private void insertRecord(Connection tx, String semaphoreName, String identifier) {
        // logger.error("insertRecord %s, %s", semaphoreName, identifier);
        String sql = "INSERT INTO semaphore(semaphore_name, holder_name) " + 
                     "VALUES(?, ?);";
        execute(tx, sql, q -> q.addParameter(semaphoreName).addParameter(identifier).executeUpdate());
    }

    private void deleteRecord(Connection tx, String semaphoreName, String identifier) {
        // logger.error("deleteRecord %s, %s", semaphoreName, identifier);
        String sql = "DELETE FROM semaphore " + 
                     "WHERE semaphore_name = ? and holder_name = ? ;";
        execute(tx, sql, q -> q.addParameter(semaphoreName).addParameter(identifier).executeDelete());
    }

    private void deleteExpired(Connection tx, String semaphoreName, int timeoutMicroseconds) {
        // logger.error("deleteRecord %s, %d", semaphoreName, timeoutMicroseconds);
        String sql = "DELETE FROM semaphore " + 
                     "WHERE STRCMP(semaphore_name, ?) = 0 and TIMESTAMPDIFF( MICROSECOND, created_on, NOW(3) ) > ? ;";
        execute(tx, sql, q -> q.addParameter(semaphoreName).addParameter(timeoutMicroseconds).executeDelete());
    }

    private boolean deleteRecordIfExceedsLimit(String semaphoreName, String identifier, int limit) {
        // logger.error("deleteRecord %s, %s, %s", semaphoreName, identifier, limit);
        String sql =  "SELECT a.count FROM ( " + 
                      "    SELECT "+
                      "        ROW_NUMBER() OVER(ORDER BY created_on ASC) AS count, " +
                      "        holder_name " +
                      "    FROM semaphore " +
                      "    WHERE semaphore_name = '%s' "+ 
                      ") AS a WHERE a.holder_name = '%s';";
        sql = String.format(sql, semaphoreName, identifier);
        // logger.error("deleteRecord: sql: %s", sql);
        int running = queryWithTransaction(sql, q -> q.executeAndFetchFirst(Integer.class)).intValue();
        logger.info("deleteRecord: running: {}, limit: {}", running, limit);
        if (running > limit) {
            withTransaction(tx -> deleteRecord(tx, semaphoreName, identifier));
            return false;
        }
        return true;
    }
}