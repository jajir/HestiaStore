package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates active WAL replay, append, checkpoint, retention, and failure
 * handling.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class ActiveWalCoordinator<K, V>
        implements WalCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(ActiveWalCoordinator.class);

    private final WalRuntime<K, V> walRuntime;
    private final AtomicLong lastAppliedWalLsn;
    private final WalBackpressureCoordinator<K, V> retentionPressureCoordinator;
    private final SegmentIndexRuntimeState runtimeState;

    ActiveWalCoordinator(
            final EffectiveIndexConfiguration<K, V> configuration,
            final WalRuntime<K, V> walRuntime,
            final BusyRetryPolicy retryPolicy,
            final WalCheckpointMaintenance checkpointMaintenance,
            final SegmentIndexRuntimeState runtimeState,
            final AtomicLong lastAppliedWalLsn) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        retentionPressureCoordinator = new WalBackpressureCoordinator<>(
                configuration, walRuntime, retryPolicy, checkpointMaintenance,
                lastAppliedWalLsn);
        this.runtimeState = Vldtn.requireNonNull(runtimeState, "runtimeState");
    }

    @Override
    public void recover(
            final Consumer<WalRuntime.ReplayRecord<K, V>> replayConsumer) {
        final WalRuntime.RecoveryResult recoveryResult = walRuntime.recover(
                replayConsumer);
        if (recoveryResult.maxLsn() > 0L) {
            lastAppliedWalLsn.accumulateAndGet(recoveryResult.maxLsn(),
                    Math::max);
        }
    }

    @Override
    public void checkpoint() {
        try {
            checkpointInternal();
        } catch (final RuntimeException failure) {
            throw propagateWalFailure(failure);
        }
    }

    @Override
    public long appendPut(final K key, final V value) {
        try {
            retentionPressureCoordinator.enforceIfNeeded();
            return walRuntime.appendPut(key, value);
        } catch (final RuntimeException failure) {
            throw propagateWalFailure(failure);
        }
    }

    @Override
    public long appendDelete(final K key) {
        try {
            retentionPressureCoordinator.enforceIfNeeded();
            return walRuntime.appendDelete(key);
        } catch (final RuntimeException failure) {
            throw propagateWalFailure(failure);
        }
    }

    @Override
    public void recordAppliedLsn(final long walLsn) {
        if (walLsn > 0L) {
            lastAppliedWalLsn.accumulateAndGet(walLsn, Math::max);
        }
    }

    @Override
    public void close() {
        walRuntime.close();
    }

    private void checkpointInternal() {
        walRuntime.onCheckpoint(lastAppliedWalLsn.get());
        logCheckpointStatsIfEnabled();
    }

    private void logCheckpointStatsIfEnabled() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        final var walMonitoring = walRuntime.statsSnapshot();
        LOGGER.debug(
                "WAL checkpoint: durableLsn={}, checkpointLsn={}, retainedBytes={}, segments={}",
                walMonitoring.durableLsn(), walMonitoring.checkpointLsn(),
                walMonitoring.retainedBytes(), walMonitoring.segmentCount());
    }

    private RuntimeException propagateWalFailure(final RuntimeException failure) {
        if (!walRuntime.hasSyncFailure()) {
            return failure;
        }
        final SegmentIndexState state = runtimeState.currentState();
        if (state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR) {
            return failure;
        }
        LOGGER.error(
                "event=wal_sync_failure_transition state={} action=transition_to_error reason=wal_sync_failure",
                state, failure);
        runtimeState.markRuntimeFailure(failure);
        return failure;
    }
}
