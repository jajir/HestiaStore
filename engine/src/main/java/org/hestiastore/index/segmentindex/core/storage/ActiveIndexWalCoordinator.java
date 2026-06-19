package org.hestiastore.index.segmentindex.core.storage;

import java.util.function.LongSupplier;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Coordinates active WAL replay, append, checkpoint, retention, and failure
 * handling.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class ActiveIndexWalCoordinator<K, V>
        implements IndexWalCoordinatorDelegate<K, V> {

    private final WalRuntime<K, V> walRuntime;
    private final WalReplayProgressTracker<K, V> walReplayProgressTracker;
    private final WalCheckpointExecutor<K, V> walCheckpointExecutor;
    private final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator;
    private final WalFailureTransitionHandler walFailureTransitionHandler;

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

    static <K, V> ActiveIndexWalCoordinator<K, V> create(
            final WalRuntimeInitialization<K, V> initialization,
            final BusyRetryPolicy retryPolicy) {
        final WalRuntimeInitialization<K, V> walInitialization =
                Vldtn.requireNonNull(initialization, "initialization");
        final WalRuntime<K, V> walRuntime = walInitialization.walRuntime();
        final WalReplayProgressTracker<K, V> replayProgressTracker =
                new WalReplayProgressTracker<>(walRuntime,
                        walInitialization.lastAppliedWalLsn());
        final WalFailureTransitionHandler failureTransitionHandler =
                new WalFailureTransitionHandler(walRuntime,
                        walInitialization.stateView(),
                        walInitialization.failureHandler());
        final WalCheckpointExecutor<K, V> checkpointExecutor =
                new WalCheckpointExecutor<>(walRuntime,
                        walInitialization.lastAppliedWalLsn(),
                        failureTransitionHandler);
        final WalRetentionPressureCheckpoint<K, V> retentionPressureCheckpoint =
                new WalRetentionPressureCheckpoint<>(
                        walInitialization.durableState(), checkpointExecutor);
        final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator =
                new WalRetentionPressureCoordinator<>(
                        walInitialization.configuration(), walRuntime,
                        retryPolicy, retentionPressureCheckpoint);
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
