package org.hestiastore.index.segmentindex.core.maintenance;

/**
 * Maintenance-facing synchronization view over split runtime internals.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SplitMaintenanceSynchronization<K, V> {

    /**
     * Requests a full split-policy scan regardless of current in-flight split
     * state.
     */
    void requestFullSplitScan();

    /**
     * Waits until split-policy work and in-flight splits are quiescent.
     */
    void awaitQuiescence();

}
