package com.netflix.conductor.dao;

public interface SemaphoreDAO {

    /**
     * Try to acquire the semaphore
     * @param semaphoreName: The name of the semaphore
     * @param identifier: A unique identifier that identifies the holder
     * @param limit: This semaphore's maximum number of holders limit. If changes between calls for the same semaphore name, behavior is undefined.
     * @param timeoutSeconds: A timeout for all the holders of the semaphore If changes between calls for the same semaphore name, behavior is undefined.
     * @return true: If the semaphore was successfully acquired
     * 		false: If the semaphore was successfully not acquired (e.g. Reached limit)
     */
    boolean tryAcquire(String semaphoreName, String identifier, int limit, double timeoutSeconds);

    /**
     * Release the semaphore
     * @param semaphoreName: The name of the semaphore
     * @param identifier: A unique identifier that identifies the holder
     * @return true: If the semaphore was successfully released
     * 		false: If the semaphore was successfully not released (e.g. Never acquired in the first place)
     */
    boolean release(String semaphoreName, String identifier);
}