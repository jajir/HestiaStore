package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
final class IndexWalCoordinator<K, V> {

    private static final long WAL_RETENTION_PRESSURE_WARN_INTERVAL_NANOS = TimeUnit.SECONDS
            .toNanos(5L);

    private final Logger logger;
    private final IndexConfiguration<K, V> conf;
    private final WalRuntime<K, V> walRuntime;
    private final IndexRetryPolicy retryPolicy;
    private final Runnable prepareDurableStateAction;
    private final Runnable flushDurableStateAction;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Consumer<RuntimeException> failureHandler;
    private final AtomicLong lastAppliedWalLsn;
    private final AtomicLong walRetentionPressureLastWarnNanos = new AtomicLong(
            0L);
    private final AtomicBoolean walRetentionPressureWarnActive = new AtomicBoolean(
            false);

    IndexWalCoordinator(final Logger logger,
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
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
    }

    void recover(final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        if (!walRuntime.isEnabled()) {
            return;
        }
        final WalRuntime.RecoveryResult recoveryResult = walRuntime
                .recover(replayConsumer);
        if (recoveryResult.maxLsn() > 0L) {
            lastAppliedWalLsn.accumulateAndGet(recoveryResult.maxLsn(),
                    Math::max);
        }
    }

    void checkpoint() {
        if (!walRuntime.isEnabled()) {
            return;
        }
        try {
            walRuntime.onCheckpoint(lastAppliedWalLsn.get());
            if (logger.isDebugEnabled()) {
                final var walStats = walRuntime.statsSnapshot();
                logger.debug(
                        "WAL checkpoint: durableLsn={}, checkpointLsn={}, retainedBytes={}, segments={}",
                        walStats.durableLsn(), walStats.checkpointLsn(),
                        walStats.retainedBytes(), walStats.segmentCount());
            }
        } catch (final RuntimeException failure) {
            handleWalRuntimeFailure(failure);
            throw failure;
        }
    }

    long appendPut(final K key, final V value) {
        if (!walRuntime.isEnabled()) {
            return 0L;
        }
        try {
            enforceWalRetentionPressureIfNeeded();
            return walRuntime.appendPut(key, value);
        } catch (final RuntimeException failure) {
            handleWalRuntimeFailure(failure);
            throw failure;
        }
    }

    long appendDelete(final K key) {
        if (!walRuntime.isEnabled()) {
            return 0L;
        }
        try {
            enforceWalRetentionPressureIfNeeded();
            return walRuntime.appendDelete(key);
        } catch (final RuntimeException failure) {
            handleWalRuntimeFailure(failure);
            throw failure;
        }
    }

    void recordAppliedLsn(final long walLsn) {
        if (walLsn <= 0L || !walRuntime.isEnabled()) {
            return;
        }
        while (true) {
            final long current = lastAppliedWalLsn.get();
            if (walLsn <= current) {
                return;
            }
            if (lastAppliedWalLsn.compareAndSet(current, walLsn)) {
                return;
            }
        }
    }

    private void enforceWalRetentionPressureIfNeeded() {
        if (!walRuntime.isEnabled() || !walRuntime.isRetentionPressure()) {
            return;
        }
        logWalRetentionPressureStartIfNeeded();
        final long startNanos = retryPolicy.startNanos();
        int checkpointAttempts = 0;
        while (walRuntime.isRetentionPressure()) {
            checkpointAttempts++;
            prepareDurableStateAction.run();
            flushDurableStateAction.run();
            checkpoint();
            if (!walRuntime.isRetentionPressure()) {
                logWalRetentionPressureCleared(startNanos, checkpointAttempts);
                return;
            }
            retryPolicy.backoffOrThrow(startNanos, "walBackpressure", null);
        }
        logWalRetentionPressureCleared(startNanos, checkpointAttempts);
    }

    private void logWalRetentionPressureStartIfNeeded() {
        final long nowNanos = System.nanoTime();
        while (true) {
            final long previousWarnNanos = walRetentionPressureLastWarnNanos
                    .get();
            if (previousWarnNanos != 0L && nowNanos
                    - previousWarnNanos < WAL_RETENTION_PRESSURE_WARN_INTERVAL_NANOS) {
                return;
            }
            if (walRetentionPressureLastWarnNanos
                    .compareAndSet(previousWarnNanos, nowNanos)) {
                walRetentionPressureWarnActive.set(true);
                logger.warn(
                        "event=wal_retention_pressure_start retainedBytes={} threshold={} action=force_checkpoint_backpressure",
                        walRuntime.retainedBytes(),
                        conf.getWal().getMaxBytesBeforeForcedCheckpoint());
                return;
            }
        }
    }

    private void logWalRetentionPressureCleared(final long startNanos,
            final int checkpointAttempts) {
        if (!walRetentionPressureWarnActive.compareAndSet(true, false)) {
            return;
        }
        final long elapsedMillis = TimeUnit.NANOSECONDS
                .toMillis(Math.max(0L, System.nanoTime() - startNanos));
        logger.info(
                "event=wal_retention_pressure_cleared retainedBytes={} threshold={} checkpointAttempts={} elapsedMillis={}",
                walRuntime.retainedBytes(),
                conf.getWal().getMaxBytesBeforeForcedCheckpoint(),
                Math.max(0, checkpointAttempts), Math.max(0L, elapsedMillis));
    }

    private void handleWalRuntimeFailure(final RuntimeException failure) {
        if (!walRuntime.isEnabled() || !walRuntime.hasSyncFailure()) {
            return;
        }
        final SegmentIndexState state = stateSupplier.get();
        if (state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR) {
            return;
        }
        logger.error(
                "event=wal_sync_failure_transition state={} action=transition_to_error reason=wal_sync_failure",
                state, failure);
        failureHandler.accept(failure);
    }
}
