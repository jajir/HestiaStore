package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes WAL checkpoints and emits checkpoint diagnostics.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class WalCheckpointExecutor<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(WalCheckpointExecutor.class);

    private final WalRuntime<K, V> walRuntime;
    private final AtomicLong lastAppliedWalLsn;
    private final WalFailureTransitionHandler walFailureTransitionHandler;

    WalCheckpointExecutor(
            final WalRuntime<K, V> walRuntime,
            final AtomicLong lastAppliedWalLsn,
            final WalFailureTransitionHandler walFailureTransitionHandler) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.walFailureTransitionHandler = Vldtn.requireNonNull(
                walFailureTransitionHandler, "walFailureTransitionHandler");
    }

    void checkpoint() {
        try {
            checkpointInternal();
        } catch (final RuntimeException failure) {
            throw walFailureTransitionHandler.propagate(failure);
        }
    }

    void checkpointInternal() {
        walRuntime.onCheckpoint(lastAppliedWalLsn.get());
        logCheckpointStatsIfEnabled();
    }

    private void logCheckpointStatsIfEnabled() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        final var walStats = walRuntime.statsSnapshot();
        LOGGER.debug(
                "WAL checkpoint: durableLsn={}, checkpointLsn={}, retainedBytes={}, segments={}",
                walStats.durableLsn(), walStats.checkpointLsn(),
                walStats.retainedBytes(), walStats.segmentCount());
    }
}
