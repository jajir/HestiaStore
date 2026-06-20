package org.hestiastore.index.segmentindex.core.storage;

/**
 * Flushes index runtime state before a WAL checkpoint can safely advance.
 */
public interface WalCheckpointMaintenance {

    /**
     * Flushes runtime state and waits until the durable state is settled.
     */
    void flushAndWait();
}
