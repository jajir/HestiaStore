package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory
            .getLogger(WalRetentionPressureCoordinator.class);

    private final EffectiveIndexConfiguration<K, V> conf;
    private final WalRuntime<K, V> walRuntime;
    private final WalBackpressureRetryPolicy retryPolicy;
    private final WalRetentionPressureCheckpoint<K, V> checkpoint;
    private final AtomicLong walRetentionPressureLastWarnNanos = new AtomicLong(
            0L);
    private final AtomicBoolean walRetentionPressureWarnActive = new AtomicBoolean(
            false);

    WalRetentionPressureCoordinator(
            final EffectiveIndexConfiguration<K, V> conf,
            final WalRuntime<K, V> walRuntime,
            final WalBackpressureRetryPolicy retryPolicy,
            final WalRetentionPressureCheckpoint<K, V> checkpoint) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.checkpoint = Vldtn.requireNonNull(checkpoint, "checkpoint");
    }

    void enforceIfNeeded() {
        if (!walRuntime.isRetentionPressure()) {
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
        checkpoint.forceCheckpoint();
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
                LOGGER.warn(
                        "event=wal_retention_pressure_start retainedBytes={} threshold={} action=force_checkpoint_backpressure",
                        walRuntime.retainedBytes(),
                        conf.wal().getMaxBytesBeforeForcedCheckpoint());
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
        LOGGER.info(
                "event=wal_retention_pressure_cleared retainedBytes={} threshold={} checkpointAttempts={} elapsedMillis={}",
                walRuntime.retainedBytes(),
                conf.wal().getMaxBytesBeforeForcedCheckpoint(),
                Math.max(0, checkpointAttempts), Math.max(0L, elapsedMillis));
    }
}
