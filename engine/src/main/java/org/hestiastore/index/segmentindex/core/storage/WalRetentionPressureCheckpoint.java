package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;

/**
 * Forces durable index state and WAL checkpoint state to catch up during WAL
 * retention pressure.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class WalRetentionPressureCheckpoint<K, V> {

    private final WalCheckpointDurableState durableState;
    private final WalCheckpointExecutor<K, V> checkpointExecutor;

    WalRetentionPressureCheckpoint(
            final WalCheckpointDurableState durableState,
            final WalCheckpointExecutor<K, V> checkpointExecutor) {
        this.durableState = Vldtn.requireNonNull(durableState,
                "durableState");
        this.checkpointExecutor = Vldtn.requireNonNull(checkpointExecutor,
                "checkpointExecutor");
    }

    void forceCheckpoint() {
        durableState.flushBeforeWalCheckpoint();
        checkpointExecutor.checkpointInternal();
    }
}
