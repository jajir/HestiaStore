package org.hestiastore.index.segmentindex.core.storage;

/**
 * Flushes index state that must be durable before WAL checkpoint retention can
 * safely advance.
 */
public interface WalCheckpointDurableState {

    /**
     * Flushes durable index state before WAL checkpointing.
     */
    void flushBeforeWalCheckpoint();
}
