package org.hestiastore.index.segmentindex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Collects immutable metrics snapshots for the index runtime.
 */
@SuppressWarnings("java:S107")
final class SegmentIndexMetricsCollector<K, V> {

    private final SegmentRegistry<K, V> segmentRegistry;
    private final StableSegmentRuntimeCollector<K, V> stableSegmentRuntimeCollector;
    private final ExecutorRegistry executorRegistry;
    private final WalRuntime<K, V> walRuntime;
    private final Stats stats;
    private final AtomicLong compactRequestHighWaterMark;
    private final AtomicLong flushRequestHighWaterMark;
    private final SegmentIndexMetricsSnapshotFactory<K, V> snapshotFactory;

    private SegmentIndexMetricsCollector(
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentRuntimeCollector<K, V> stableSegmentRuntimeCollector,
            final ExecutorRegistry executorRegistry,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final SegmentIndexMetricsSnapshotFactory<K, V> snapshotFactory) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentRuntimeCollector = Vldtn.requireNonNull(
                stableSegmentRuntimeCollector, "stableSegmentRuntimeCollector");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.compactRequestHighWaterMark = Vldtn.requireNonNull(
                compactRequestHighWaterMark, "compactRequestHighWaterMark");
        this.flushRequestHighWaterMark = Vldtn.requireNonNull(
                flushRequestHighWaterMark, "flushRequestHighWaterMark");
        this.snapshotFactory = Vldtn.requireNonNull(snapshotFactory,
                "snapshotFactory");
    }

    public static <K, V> SegmentIndexMetricsCollector<K, V> create(
            final IndexConfiguration<K, V> conf,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Supplier<SplitMetricsSnapshot> splitSnapshotSupplier,
            final ExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        return new SegmentIndexMetricsCollector<>(
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                new StableSegmentRuntimeCollector<>(
                        Vldtn.requireNonNull(keyToSegmentMap,
                                "keyToSegmentMap"),
                        Vldtn.requireNonNull(segmentRegistry,
                                "segmentRegistry")),
                Vldtn.requireNonNull(executorRegistry, "executorRegistry"),
                Vldtn.requireNonNull(walRuntime, "walRuntime"),
                Vldtn.requireNonNull(stats, "stats"),
                Vldtn.requireNonNull(compactRequestHighWaterMark,
                        "compactRequestHighWaterMark"),
                Vldtn.requireNonNull(flushRequestHighWaterMark,
                        "flushRequestHighWaterMark"),
                newSnapshotFactory(conf, splitSnapshotSupplier,
                        runtimeTuningState, walRuntime, stats,
                        lastAppliedWalLsn, stateSupplier));
    }

    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        final StableSegmentRuntimeMetrics stableSegmentRuntime =
                stableSegmentRuntimeCollector.collect();
        final IndexExecutorRuntimeAccess executorSnapshot =
                executorRegistry.runtimeSnapshot();
        return snapshotFactory.create(segmentRegistry.metricsSnapshot(),
                stableSegmentRuntime, executorSnapshot,
                walRuntime.statsSnapshot(),
                resolveRequestCount(0L, compactRequestHighWaterMark,
                        stableSegmentRuntime.getTotalCompactRequestCount()),
                resolveRequestCount(stats.getFlushRequestCount(),
                        flushRequestHighWaterMark,
                        stableSegmentRuntime.getTotalFlushRequestCount()));
    }

    private static <K, V> SegmentIndexMetricsSnapshotFactory<K, V> newSnapshotFactory(
            final IndexConfiguration<K, V> conf,
            final Supplier<SplitMetricsSnapshot> splitSnapshotSupplier,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        return new SegmentIndexMetricsSnapshotFactory<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(splitSnapshotSupplier,
                        "splitSnapshotSupplier"),
                Vldtn.requireNonNull(runtimeTuningState, "runtimeTuningState"),
                Vldtn.requireNonNull(walRuntime, "walRuntime"),
                Vldtn.requireNonNull(stats, "stats"),
                Vldtn.requireNonNull(lastAppliedWalLsn, "lastAppliedWalLsn"),
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"));
    }

    private static long resolveRequestCount(final long fallbackCount,
            final AtomicLong highWaterMark, final long observedCount) {
        return Math.max(fallbackCount,
                updateHighWaterMark(highWaterMark, observedCount));
    }

    private static long updateHighWaterMark(final AtomicLong highWaterMark,
            final long observedValue) {
        final long sanitizedValue = Math.max(0L, observedValue);
        while (true) {
            final long currentValue = highWaterMark.get();
            if (sanitizedValue <= currentValue) {
                return currentValue;
            }
            if (highWaterMark.compareAndSet(currentValue, sanitizedValue)) {
                return sanitizedValue;
            }
        }
    }
}
