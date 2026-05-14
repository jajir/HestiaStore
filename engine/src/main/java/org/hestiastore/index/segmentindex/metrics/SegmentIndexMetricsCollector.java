package org.hestiastore.index.segmentindex.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryStats;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
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
    private final Supplier<MaintenanceStats> maintenanceStatsSupplier;
    private final AtomicLong compactRequestHighWaterMark;
    private final AtomicLong flushRequestHighWaterMark;
    private final SegmentIndexMetricsSnapshotFactory<K, V> snapshotFactory;

    private SegmentIndexMetricsCollector(
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentRuntimeCollector<K, V> stableSegmentRuntimeCollector,
            final ExecutorRegistry executorRegistry,
            final WalRuntime<K, V> walRuntime,
            final Supplier<MaintenanceStats> maintenanceStatsSupplier,
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
        this.maintenanceStatsSupplier = Vldtn.requireNonNull(
                maintenanceStatsSupplier, "maintenanceStatsSupplier");
        this.compactRequestHighWaterMark = Vldtn.requireNonNull(
                compactRequestHighWaterMark, "compactRequestHighWaterMark");
        this.flushRequestHighWaterMark = Vldtn.requireNonNull(
                flushRequestHighWaterMark, "flushRequestHighWaterMark");
        this.snapshotFactory = Vldtn.requireNonNull(snapshotFactory,
                "snapshotFactory");
    }

    static <K, V> SegmentIndexMetricsCollector<K, V> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Supplier<SplitStats> splitStatsSupplier,
            final ExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final WalRuntime<K, V> walRuntime,
            final Supplier<IndexOperationStats> indexOperationStatsSupplier,
            final Supplier<MaintenanceStats> maintenanceStatsSupplier,
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
                Vldtn.requireNonNull(maintenanceStatsSupplier,
                        "maintenanceStatsSupplier"),
                Vldtn.requireNonNull(compactRequestHighWaterMark,
                        "compactRequestHighWaterMark"),
                Vldtn.requireNonNull(flushRequestHighWaterMark,
                        "flushRequestHighWaterMark"),
                newSnapshotFactory(conf, splitStatsSupplier,
                        runtimeTuningState, chunkStoreCache, walRuntime,
                        indexOperationStatsSupplier, lastAppliedWalLsn,
                        stateSupplier));
    }

    SegmentIndexMetricsSnapshot metricsSnapshot() {
        final StableSegmentRuntimeMetrics stableSegmentRuntime =
                stableSegmentRuntimeCollector.collect();
        final ExecutorRegistryStats executorSnapshot =
                executorRegistry.statsSnapshot();
        final MaintenanceStats maintenanceStats =
                maintenanceStatsSupplier.get();
        return snapshotFactory.create(segmentRegistry.metricsSnapshot(),
                stableSegmentRuntime, executorSnapshot,
                walRuntime.statsSnapshot(), maintenanceStats,
                resolveRequestCount(maintenanceStats.getCompactRequestCount(),
                        compactRequestHighWaterMark,
                        stableSegmentRuntime.getTotalCompactRequestCount()),
                resolveRequestCount(maintenanceStats.getFlushRequestCount(),
                        flushRequestHighWaterMark,
                        stableSegmentRuntime.getTotalFlushRequestCount()));
    }

    private static <K, V> SegmentIndexMetricsSnapshotFactory<K, V> newSnapshotFactory(
            final EffectiveIndexConfiguration<K, V> conf,
            final Supplier<SplitStats> splitStatsSupplier,
            final RuntimeTuningState runtimeTuningState,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final WalRuntime<K, V> walRuntime,
            final Supplier<IndexOperationStats> indexOperationStatsSupplier,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        return new SegmentIndexMetricsSnapshotFactory<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(splitStatsSupplier,
                        "splitStatsSupplier"),
                Vldtn.requireNonNull(chunkStoreCache, "chunkStoreCache")
                        ::stats,
                Vldtn.requireNonNull(runtimeTuningState, "runtimeTuningState"),
                Vldtn.requireNonNull(walRuntime, "walRuntime"),
                Vldtn.requireNonNull(indexOperationStatsSupplier,
                        "indexOperationStatsSupplier"),
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
