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

}
