package org.hestiastore.index.segment;

import java.util.function.Supplier;

/**
 * Optional interface for segments that can execute operations under internal
 * write locks.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentWriteLockSupport<K, V> {

    /**
     * Executes a task under the segment write lock.
     *
     * @param task task to execute
     * @param <T>  result type
     * @return result of the task
     */
    <T> T executeWithWriteLock(Supplier<T> task);

    /**
     * Executes a task under the maintenance + write locks.
     *
     * @param task task to execute
     * @param <T>  result type
     * @return result of the task
     */
    <T> T executeWithMaintenanceWriteLock(Supplier<T> task);

    /**
     * Validates a condition under the write lock and writes when valid.
     *
     * @param validation validation to execute under lock
     * @param key        key to store
     * @param value      value to store
     * @return true when the entry was written
     */
    boolean putIfValid(Supplier<Boolean> validation, K key, V value);
}
