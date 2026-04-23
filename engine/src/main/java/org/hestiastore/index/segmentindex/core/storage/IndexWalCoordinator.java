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

    private final Logger logger;
    private final IndexConfiguration<K, V> conf;
    private final WalRuntime<K, V> walRuntime;
    private final IndexRetryPolicy retryPolicy;
    private final Runnable prepareDurableStateAction;
    private final Runnable flushDurableStateAction;
    private final AtomicLong lastAppliedWalLsn;
    private final WalReplayProgressTracker<K, V> walReplayProgressTracker;
    private final WalCheckpointExecutor<K, V> walCheckpointExecutor;
    private final WalRetentionPressureCoordinator<K, V> retentionPressureCoordinator;
    private final WalFailureTransitionHandler walFailureTransitionHandler;

    /**
     * Creates a coordinator for WAL replay, append, checkpoint, and failure
     * handling concerns.
     *
     * @param logger runtime logger
     * @param conf index configuration
     * @param walRuntime WAL runtime
     * @param retryPolicy retry policy used under retention pressure
     * @param prepareDurableStateAction callback run before checkpointing
     * @param flushDurableStateAction callback run to flush durable state
     * @param stateSupplier runtime state supplier
     * @param failureHandler runtime failure handler
     * @param lastAppliedWalLsn mutable last-applied LSN holder
     */
    public IndexWalCoordinator(final Logger logger,
            final IndexConfiguration<K, V> conf,
            final WalRuntime<K, V> walRuntime,
            final IndexRetryPolicy retryPolicy,
            final Runnable prepareDurableStateAction,
            final Runnable flushDurableStateAction,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler,
            final AtomicLong lastAppliedWalLsn) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.prepareDurableStateAction = Vldtn.requireNonNull(
                prepareDurableStateAction, "prepareDurableStateAction");
        this.flushDurableStateAction = Vldtn.requireNonNull(
                flushDurableStateAction, "flushDurableStateAction");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.walReplayProgressTracker = new WalReplayProgressTracker<>(
                this.walRuntime, this.lastAppliedWalLsn);
        this.retentionPressureCoordinator = new WalRetentionPressureCoordinator<>(
                this.logger, this.conf, this.walRuntime, this.retryPolicy,
                this.prepareDurableStateAction, this.flushDurableStateAction,
                this::checkpointInternal);
        this.walFailureTransitionHandler = new WalFailureTransitionHandler(
                this.logger, this.walRuntime,
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                Vldtn.requireNonNull(failureHandler, "failureHandler"));
        this.walCheckpointExecutor = new WalCheckpointExecutor<>(this.logger,
                this.walRuntime, this.lastAppliedWalLsn,
                this.walFailureTransitionHandler);
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

    private void checkpointInternal() {
        walCheckpointExecutor.checkpointInternal();
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
