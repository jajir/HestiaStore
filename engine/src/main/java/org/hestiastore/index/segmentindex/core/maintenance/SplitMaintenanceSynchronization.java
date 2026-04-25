package org.hestiastore.index.segmentindex.core.maintenance;

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
     * Requests split-policy reconciliation regardless of current in-flight
     * split state.
     */
    void requestReconciliation();

    /**
     * Requests split-policy reconciliation only when no split is currently in
     * flight.
     */
    void requestReconciliationIfIdle();

    /**
     * Waits until split-policy work and in-flight splits are quiescent.
     */
    void awaitQuiescence();

    /**
     * Runs an action while new split scheduling is paused.
     *
     * @param action action to run
     */
    @Deprecated
    void runWithSplitSchedulingPaused(Runnable action);
}
