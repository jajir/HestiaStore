package org.hestiastore.index.segmentindex.core.durability;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;

/**
 * Handles WAL retention pressure backpressure, warning cadence, and forced
 * durable checkpoints.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class WalRetentionPressureCoordinator<K, V> {

    private static final long WAL_RETENTION_PRESSURE_WARN_INTERVAL_NANOS = TimeUnit.SECONDS
            .toNanos(5L);

    private final Logger logger;
    private final IndexConfiguration<K, V> conf;
    private final WalRuntime<K, V> walRuntime;
    private final IndexRetryPolicy retryPolicy;
    private final Runnable prepareDurableStateAction;
    private final Runnable flushDurableStateAction;
    private final Runnable checkpointAction;
    private final AtomicLong walRetentionPressureLastWarnNanos = new AtomicLong(
            0L);
    private final AtomicBoolean walRetentionPressureWarnActive = new AtomicBoolean(
            false);

    WalRetentionPressureCoordinator(final Logger logger,
            final IndexConfiguration<K, V> conf,
            final WalRuntime<K, V> walRuntime,
            final IndexRetryPolicy retryPolicy,
            final Runnable prepareDurableStateAction,
            final Runnable flushDurableStateAction,
            final Runnable checkpointAction) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.prepareDurableStateAction = Vldtn.requireNonNull(
                prepareDurableStateAction, "prepareDurableStateAction");
        this.flushDurableStateAction = Vldtn.requireNonNull(
                flushDurableStateAction, "flushDurableStateAction");
        this.checkpointAction = Vldtn.requireNonNull(checkpointAction,
                "checkpointAction");
    }

    void enforceIfNeeded() {
        if (!walRuntime.isEnabled() || !walRuntime.isRetentionPressure()) {
            return;
        }
        logWalRetentionPressureStartIfNeeded();
        final long startNanos = retryPolicy.startNanos();
        int checkpointAttempts = 0;
        while (walRuntime.isRetentionPressure()) {
            checkpointAttempts++;
            forceDurableStateCheckpoint();
            if (!walRuntime.isRetentionPressure()) {
                logWalRetentionPressureCleared(startNanos, checkpointAttempts);
                return;
            }
            retryPolicy.backoffOrThrow(startNanos, "walBackpressure", null);
        }
        logWalRetentionPressureCleared(startNanos, checkpointAttempts);
    }

    private void forceDurableStateCheckpoint() {
        prepareDurableStateAction.run();
        flushDurableStateAction.run();
        checkpointAction.run();
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
}
