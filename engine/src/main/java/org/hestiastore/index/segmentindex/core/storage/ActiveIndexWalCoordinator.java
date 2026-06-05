package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Coordinates active WAL replay, append, checkpoint, retention, and failure
 * handling.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("java:S107")
final class ActiveIndexWalCoordinator<K, V>
        implements IndexWalCoordinatorDelegate<K, V> {

    private final WalRuntime<K, V> walRuntime;
    private final WalReplayProgressTracker<K, V> walReplayProgressTracker;
    private final WalCheckpointExecutor<K, V> walCheckpointExecutor;
    private final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator;
    private final WalFailureTransitionHandler walFailureTransitionHandler;

    @SuppressWarnings("java:S107")
    private ActiveIndexWalCoordinator(final WalRuntime<K, V> walRuntime,
            final WalReplayProgressTracker<K, V> walReplayProgressTracker,
            final WalCheckpointExecutor<K, V> walCheckpointExecutor,
            final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator,
            final WalFailureTransitionHandler walFailureTransitionHandler) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.walReplayProgressTracker = Vldtn.requireNonNull(
                walReplayProgressTracker, "walReplayProgressTracker");
        this.walCheckpointExecutor = Vldtn.requireNonNull(
                walCheckpointExecutor, "walCheckpointExecutor");
        this.retentionPressureCoordinator = Vldtn.requireNonNull(
                retentionPressureCoordinator, "retentionPressureCoordinator");
        this.walFailureTransitionHandler = Vldtn.requireNonNull(
                walFailureTransitionHandler, "walFailureTransitionHandler");
    }

    @SuppressWarnings("java:S107")
    static <K, V> ActiveIndexWalCoordinator<K, V> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final WalRuntime<K, V> walRuntime,
            final IndexRetryPolicy retryPolicy,
            final Runnable prepareDurableStateAction,
            final Runnable flushDurableStateAction,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler,
            final AtomicLong lastAppliedWalLsn) {
        final WalReplayProgressTracker<K, V> replayProgressTracker =
                new WalReplayProgressTracker<>(walRuntime, lastAppliedWalLsn);
        final WalFailureTransitionHandler failureTransitionHandler =
                new WalFailureTransitionHandler(walRuntime, stateSupplier,
                        failureHandler);
        final WalCheckpointExecutor<K, V> checkpointExecutor =
                new WalCheckpointExecutor<>(walRuntime, lastAppliedWalLsn,
                        failureTransitionHandler);
        final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator =
                new WalRetentionPressureCoordinator<>(conf, walRuntime,
                        retryPolicy, prepareDurableStateAction,
                        flushDurableStateAction,
                        checkpointExecutor::checkpointInternal);
        return new ActiveIndexWalCoordinator<>(walRuntime,
                replayProgressTracker, checkpointExecutor,
                retentionPressureCoordinator, failureTransitionHandler);
    }

    @Override
    public void recover(final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        walReplayProgressTracker.recover(replayConsumer);
    }

    @Override
    public void checkpoint() {
        walCheckpointExecutor.checkpoint();
    }

    @Override
    public long appendPut(final K key, final V value) {
        return appendWalEntry(() -> walRuntime.appendPut(key, value));
    }

    @Override
    public long appendDelete(final K key) {
        return appendWalEntry(() -> walRuntime.appendDelete(key));
    }

    @Override
    public void recordAppliedLsn(final long walLsn) {
        walReplayProgressTracker.recordAppliedLsn(walLsn);
    }

    @Override
    public void close() {
        walRuntime.close();
    }

    private long appendWalEntry(final LongSupplier appendAction) {
        try {
            retentionPressureCoordinator.enforceIfNeeded();
            return appendAction.getAsLong();
        } catch (final RuntimeException failure) {
            throw walFailureTransitionHandler.propagate(failure);
        }
    }
}
