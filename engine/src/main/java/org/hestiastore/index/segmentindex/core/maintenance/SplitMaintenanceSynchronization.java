package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.function.Supplier;

/**
 * Maintenance-facing synchronization view over split runtime internals.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SplitMaintenanceSynchronization<K, V> {

    /**
     * Waits until the split runtime is idle or the timeout expires.
     *
     * @param timeoutMillis timeout in milliseconds
     */
    void awaitIdle(long timeoutMillis);

    /**
     * @return number of scheduled or running split tasks
     */
    int splitInFlightCount();

    /**
     * Requests a split-policy scan only when no split is currently in flight.
     */
    void scheduleScanIfIdle();

    /**
     * Waits until split-policy work and in-flight splits are exhausted.
     */
    void awaitExhausted();

    /**
     * Runs an action while new split scheduling is paused.
     *
     * @param action action to run
     * @param <T> result type
     * @return action result
     */
    <T> T runWithSplitSchedulingPaused(Supplier<T> action);

    /**
     * Runs an action while new split scheduling is paused.
     *
     * @param action action to run
     */
    void runWithSplitSchedulingPaused(Runnable action);
}
