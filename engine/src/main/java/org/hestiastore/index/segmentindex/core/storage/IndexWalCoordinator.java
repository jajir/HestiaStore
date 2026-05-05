package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;

/**
 * Owns WAL replay, checkpointing, retention pressure handling, and failure
 * transition coordination for the segment index runtime.
 */
@SuppressWarnings("java:S107")
public final class IndexWalCoordinator<K, V> {

    private final WalRuntime<K, V> walRuntime;
    private final WalReplayProgressTracker<K, V> walReplayProgressTracker;
    private final WalCheckpointExecutor<K, V> walCheckpointExecutor;
    private final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator;
    private final WalFailureTransitionHandler walFailureTransitionHandler;

    @SuppressWarnings("java:S107")
    private IndexWalCoordinator(final WalRuntime<K, V> walRuntime,
            final WalReplayProgressTracker<K, V> walReplayProgressTracker,
            final WalCheckpointExecutor<K, V> walCheckpointExecutor,
            final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator,
            final WalFailureTransitionHandler walFailureTransitionHandler) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.walReplayProgressTracker = Vldtn.requireNonNull(
                walReplayProgressTracker, "walReplayProgressTracker");
        this.retentionPressureCoordinator = Vldtn.requireNonNull(
                retentionPressureCoordinator, "retentionPressureCoordinator");
        this.walFailureTransitionHandler = Vldtn.requireNonNull(
                walFailureTransitionHandler, "walFailureTransitionHandler");
        this.walCheckpointExecutor = Vldtn.requireNonNull(
                walCheckpointExecutor, "walCheckpointExecutor");
    }

    @SuppressWarnings("java:S107")
    public static <K, V> IndexWalCoordinator<K, V> create(
            final Logger logger, final IndexConfiguration<K, V> conf,
            final WalRuntime<K, V> walRuntime,
            final IndexRetryPolicy retryPolicy,
            final Runnable prepareDurableStateAction,
            final Runnable flushDurableStateAction,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler,
            final AtomicLong lastAppliedWalLsn) {
        final WalReplayProgressTracker<K, V> replayProgressTracker =
                new WalReplayProgressTracker<>(
                        Vldtn.requireNonNull(walRuntime, "walRuntime"),
                        Vldtn.requireNonNull(lastAppliedWalLsn,
                                "lastAppliedWalLsn"));
        final WalFailureTransitionHandler failureTransitionHandler =
                new WalFailureTransitionHandler(
                        Vldtn.requireNonNull(logger, "logger"), walRuntime,
                        Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                        Vldtn.requireNonNull(failureHandler,
                                "failureHandler"));
        final WalCheckpointExecutor<K, V> checkpointExecutor =
                new WalCheckpointExecutor<>(logger, walRuntime,
                        lastAppliedWalLsn, failureTransitionHandler);
        final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator =
                new WalRetentionPressureCoordinator<>(logger,
                        Vldtn.requireNonNull(conf, "conf"), walRuntime,
                        Vldtn.requireNonNull(retryPolicy, "retryPolicy"),
                        Vldtn.requireNonNull(prepareDurableStateAction,
                                "prepareDurableStateAction"),
                        Vldtn.requireNonNull(flushDurableStateAction,
                                "flushDurableStateAction"),
                        checkpointExecutor::checkpointInternal);
        return new IndexWalCoordinator<>(walRuntime, replayProgressTracker,
                checkpointExecutor, retentionPressureCoordinator,
                failureTransitionHandler);
    }

    /**
     * Replays unapplied WAL entries into the runtime.
     *
     * @param replayConsumer replay consumer invoked for each recovered entry
     */
    public void recover(final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        walReplayProgressTracker.recover(replayConsumer);
    }

    /**
     * Runs a checkpoint when WAL is enabled.
     */
    public void checkpoint() {
        if (!isWalEnabled()) {
            return;
        }
        walCheckpointExecutor.checkpoint();
    }

    /**
     * Appends one put entry to the WAL.
     *
     * @param key entry key
     * @param value entry value
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    public long appendPut(final K key, final V value) {
        return appendWalEntry(() -> walRuntime.appendPut(key, value));
    }

    /**
     * Appends one delete entry to the WAL.
     *
     * @param key deleted key
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    public long appendDelete(final K key) {
        return appendWalEntry(() -> walRuntime.appendDelete(key));
    }

    /**
     * Records the highest WAL LSN already applied to durable runtime state.
     *
     * @param walLsn applied WAL LSN
     */
    public void recordAppliedLsn(final long walLsn) {
        walReplayProgressTracker.recordAppliedLsn(walLsn);
    }

    private boolean isWalEnabled() {
        return walRuntime.isEnabled();
    }

    private long appendWalEntry(final LongSupplier appendAction) {
        if (!isWalEnabled()) {
            return 0L;
        }
        try {
            retentionPressureCoordinator.enforceIfNeeded();
            return appendAction.getAsLong();
        } catch (final RuntimeException failure) {
            throw propagateWalRuntimeFailure(failure);
        }
    }

    private RuntimeException propagateWalRuntimeFailure(
            final RuntimeException failure) {
        return walFailureTransitionHandler.propagate(failure);
    }
}
