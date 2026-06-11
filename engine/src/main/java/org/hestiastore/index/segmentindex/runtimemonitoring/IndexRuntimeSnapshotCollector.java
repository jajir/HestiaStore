package org.hestiastore.index.segmentindex.runtimemonitoring;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryStats;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStats;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsView;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Collects immutable runtime snapshots for the index runtime.
 */
@SuppressWarnings("java:S107")
final class IndexRuntimeSnapshotCollector<K, V>
        implements IndexRuntimeMonitoring {

    private final SegmentRegistry<K, V> segmentRegistry;
    private final StableSegmentRuntimeCollector<K, V> stableSegmentRuntimeCollector;
    private final ExecutorRegistry executorRegistry;
    private final WalMonitoringView walMonitoringView;
    private final MaintenanceStatsRecorder maintenanceStatsRecorder;
    private final AtomicLong compactRequestHighWaterMark;
    private final AtomicLong flushRequestHighWaterMark;
    private final IndexRuntimeSnapshotFactory<K, V> snapshotFactory;
    private final Clock clock;

    private IndexRuntimeSnapshotCollector(
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentRuntimeCollector<K, V> stableSegmentRuntimeCollector,
            final ExecutorRegistry executorRegistry,
            final WalMonitoringView walMonitoringView,
            final MaintenanceStatsRecorder maintenanceStatsRecorder,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final IndexRuntimeSnapshotFactory<K, V> snapshotFactory,
            final Clock clock) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentRuntimeCollector = Vldtn.requireNonNull(
                stableSegmentRuntimeCollector, "stableSegmentRuntimeCollector");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.walMonitoringView = Vldtn.requireNonNull(walMonitoringView,
                "walMonitoringView");
        this.maintenanceStatsRecorder = Vldtn.requireNonNull(
                maintenanceStatsRecorder, "maintenanceStatsRecorder");
        this.compactRequestHighWaterMark = Vldtn.requireNonNull(
                compactRequestHighWaterMark, "compactRequestHighWaterMark");
        this.flushRequestHighWaterMark = Vldtn.requireNonNull(
                flushRequestHighWaterMark, "flushRequestHighWaterMark");
        this.snapshotFactory = Vldtn.requireNonNull(snapshotFactory,
                "snapshotFactory");
        this.clock = Vldtn.requireNonNull(clock, "clock");
    }

    static <K, V> IndexRuntimeSnapshotCollector<K, V> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitStatsView splitStatsView,
            final ExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final WalMonitoringView walMonitoringView,
            final IndexOperationStatsRecorder indexOperationStatsRecorder,
            final MaintenanceStatsRecorder maintenanceStatsRecorder,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final SegmentIndexStateView stateView) {
        return create(conf, keyToSegmentMap, segmentRegistry, splitStatsView,
                executorRegistry, runtimeTuningState, chunkStoreCache,
                walMonitoringView, indexOperationStatsRecorder,
                maintenanceStatsRecorder, compactRequestHighWaterMark,
                flushRequestHighWaterMark, lastAppliedWalLsn, stateView,
                Clock.systemUTC());
    }

    static <K, V> IndexRuntimeSnapshotCollector<K, V> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitStatsView splitStatsView,
            final ExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final WalMonitoringView walMonitoringView,
            final IndexOperationStatsRecorder indexOperationStatsRecorder,
            final MaintenanceStatsRecorder maintenanceStatsRecorder,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final SegmentIndexStateView stateView,
            final Clock clock) {
        return new IndexRuntimeSnapshotCollector<>(
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                new StableSegmentRuntimeCollector<>(
                        Vldtn.requireNonNull(keyToSegmentMap,
                                "keyToSegmentMap"),
                        Vldtn.requireNonNull(segmentRegistry,
                                "segmentRegistry")),
                Vldtn.requireNonNull(executorRegistry, "executorRegistry"),
                Vldtn.requireNonNull(walMonitoringView, "walMonitoringView"),
                Vldtn.requireNonNull(maintenanceStatsRecorder,
                        "maintenanceStatsRecorder"),
                Vldtn.requireNonNull(compactRequestHighWaterMark,
                        "compactRequestHighWaterMark"),
                Vldtn.requireNonNull(flushRequestHighWaterMark,
                        "flushRequestHighWaterMark"),
                newSnapshotFactory(conf, splitStatsView,
                        runtimeTuningState, chunkStoreCache,
                        indexOperationStatsRecorder, lastAppliedWalLsn,
                        stateView),
                Vldtn.requireNonNull(clock, "clock"));
    }

    @Override
    public IndexRuntimeSnapshot snapshot() {
        final Instant capturedAt = clock.instant();
        final StableSegmentRuntimeMetrics stableSegmentRuntime =
                stableSegmentRuntimeCollector.collect();
        final ExecutorRegistryStats executorSnapshot =
                executorRegistry.statsSnapshot();
        final MaintenanceStats maintenanceStats =
                maintenanceStatsRecorder.statsSnapshot();
        return snapshotFactory.create(capturedAt,
                segmentRegistry.metricsSnapshot(), stableSegmentRuntime,
                executorSnapshot,
                walMonitoringView.statsSnapshot(), maintenanceStats,
                resolveRequestCount(maintenanceStats.getCompactRequestCount(),
                        compactRequestHighWaterMark,
                        stableSegmentRuntime.getTotalCompactRequestCount()),
                resolveRequestCount(maintenanceStats.getFlushRequestCount(),
                        flushRequestHighWaterMark,
                        stableSegmentRuntime.getTotalFlushRequestCount()));
    }

    private static <K, V> IndexRuntimeSnapshotFactory<K, V> newSnapshotFactory(
            final EffectiveIndexConfiguration<K, V> conf,
            final SplitStatsView splitStatsView,
            final RuntimeTuningState runtimeTuningState,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final IndexOperationStatsRecorder indexOperationStatsRecorder,
            final AtomicLong lastAppliedWalLsn,
            final SegmentIndexStateView stateView) {
        return new IndexRuntimeSnapshotFactory<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(splitStatsView, "splitStatsView"),
                Vldtn.requireNonNull(chunkStoreCache, "chunkStoreCache"),
                Vldtn.requireNonNull(runtimeTuningState, "runtimeTuningState"),
                Vldtn.requireNonNull(indexOperationStatsRecorder,
                        "indexOperationStatsRecorder"),
                Vldtn.requireNonNull(lastAppliedWalLsn, "lastAppliedWalLsn"),
                Vldtn.requireNonNull(stateView, "stateView"));
    }

    private static long resolveRequestCount(final long fallbackCount,
            final AtomicLong highWaterMark, final long observedCount) {
        return Math.max(fallbackCount,
                updateHighWaterMark(highWaterMark, observedCount));
    }

    private static long updateHighWaterMark(final AtomicLong highWaterMark,
            final long observedValue) {
        final long resolvedValue = Vldtn.requireGreaterThanOrEqualToZero(
                observedValue, "observedValue");
        while (true) {
            final long currentValue = highWaterMark.get();
            if (resolvedValue <= currentValue) {
                return currentValue;
            }
            if (highWaterMark.compareAndSet(currentValue, resolvedValue)) {
                return resolvedValue;
            }
        }
    }
}
