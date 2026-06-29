package org.hestiastore.index.segmentindex.monitoring;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryStats;
import org.hestiastore.index.segmentindex.core.execution.MaintenanceStatsSnapshot;
import org.hestiastore.index.segmentindex.core.execution.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.execution.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Collects immutable runtime snapshots for the index runtime.
 */
@SuppressWarnings("java:S107")
public final class SegmentIndexRuntimeSnapshotCollector<K, V>
        implements SegmentIndexRuntimeMonitoring {

    private static final String PROPERTY_SEGMENT_REGISTRY = "segmentRegistry";

    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentRuntimeMetricsCollector<K, V> stableSegmentRuntimeCollector;
    private final SplitRuntime<K, V> splitService;
    private final ExecutorRegistry executorRegistry;
    private final RuntimeTuningState runtimeTuningState;
    private final ChunkStoreCache<K, V> chunkStoreCache;
    private final WalMonitoringView walMonitoringView;
    private final IndexOperationStatsRecorder indexOperationStatsRecorder;
    private final MaintenanceStatsRecorder maintenanceStatsRecorder;
    private final AtomicLong compactRequestHighWaterMark;
    private final AtomicLong flushRequestHighWaterMark;
    private final AtomicLong lastAppliedWalLsn;
    private final SegmentIndexStateView stateView;
    private final IndexRuntimeSnapshotProjection<K, V> snapshotProjection;
    private final Clock clock;

    private SegmentIndexRuntimeSnapshotCollector(
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentRuntimeMetricsCollector<K, V> stableSegmentRuntimeCollector,
            final SplitRuntime<K, V> splitService,
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
            final IndexRuntimeSnapshotProjection<K, V> snapshotProjection,
            final Clock clock) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                PROPERTY_SEGMENT_REGISTRY);
        this.stableSegmentRuntimeCollector = Vldtn.requireNonNull(
                stableSegmentRuntimeCollector, "stableSegmentRuntimeCollector");
        this.splitService = Vldtn.requireNonNull(splitService,
                "splitService");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        this.walMonitoringView = Vldtn.requireNonNull(walMonitoringView,
                "walMonitoringView");
        this.indexOperationStatsRecorder = Vldtn.requireNonNull(
                indexOperationStatsRecorder, "indexOperationStatsRecorder");
        this.maintenanceStatsRecorder = Vldtn.requireNonNull(
                maintenanceStatsRecorder, "maintenanceStatsRecorder");
        this.compactRequestHighWaterMark = Vldtn.requireNonNull(
                compactRequestHighWaterMark, "compactRequestHighWaterMark");
        this.flushRequestHighWaterMark = Vldtn.requireNonNull(
                flushRequestHighWaterMark, "flushRequestHighWaterMark");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.stateView = Vldtn.requireNonNull(stateView, "stateView");
        this.snapshotProjection = Vldtn.requireNonNull(snapshotProjection,
                "snapshotProjection");
        this.clock = Vldtn.requireNonNull(clock, "clock");
    }

    /**
     * Creates a runtime snapshot collector using the system UTC clock.
     *
     * @param conf effective index configuration
     * @param keyToSegmentMap key-to-segment map
     * @param segmentRegistry segment registry
     * @param splitService split service
     * @param executorRegistry executor registry
     * @param runtimeTuningState runtime tuning state
     * @param chunkStoreCache chunk-store cache
     * @param walMonitoringView WAL monitoring source
     * @param indexOperationStatsRecorder point-operation stats recorder
     * @param maintenanceStatsRecorder maintenance stats recorder
     * @param compactRequestHighWaterMark compact request high-water mark
     * @param flushRequestHighWaterMark flush request high-water mark
     * @param lastAppliedWalLsn last applied WAL LSN
     * @param stateView index state view
     * @param <K> key type
     * @param <V> value type
     * @return runtime snapshot collector
     */
    public static <K, V> SegmentIndexRuntimeSnapshotCollector<K, V> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final SegmentRouteMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitRuntime<K, V> splitService,
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
        return create(conf, keyToSegmentMap, segmentRegistry, splitService,
                executorRegistry, runtimeTuningState, chunkStoreCache,
                walMonitoringView, indexOperationStatsRecorder,
                maintenanceStatsRecorder, compactRequestHighWaterMark,
                flushRequestHighWaterMark, lastAppliedWalLsn, stateView,
                Clock.systemUTC());
    }

    static <K, V> SegmentIndexRuntimeSnapshotCollector<K, V> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final SegmentRouteMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SplitRuntime<K, V> splitService,
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
        return new SegmentIndexRuntimeSnapshotCollector<>(
                Vldtn.requireNonNull(segmentRegistry,
                        PROPERTY_SEGMENT_REGISTRY),
                new SegmentRuntimeMetricsCollector<>(
                        Vldtn.requireNonNull(keyToSegmentMap,
                                "keyToSegmentMap"),
                        Vldtn.requireNonNull(segmentRegistry,
                                PROPERTY_SEGMENT_REGISTRY)),
                Vldtn.requireNonNull(splitService, "splitService"),
                Vldtn.requireNonNull(executorRegistry, "executorRegistry"),
                Vldtn.requireNonNull(runtimeTuningState, "runtimeTuningState"),
                Vldtn.requireNonNull(chunkStoreCache, "chunkStoreCache"),
                Vldtn.requireNonNull(walMonitoringView, "walMonitoringView"),
                Vldtn.requireNonNull(indexOperationStatsRecorder,
                        "indexOperationStatsRecorder"),
                Vldtn.requireNonNull(maintenanceStatsRecorder,
                        "maintenanceStatsRecorder"),
                Vldtn.requireNonNull(compactRequestHighWaterMark,
                        "compactRequestHighWaterMark"),
                Vldtn.requireNonNull(flushRequestHighWaterMark,
                        "flushRequestHighWaterMark"),
                Vldtn.requireNonNull(lastAppliedWalLsn, "lastAppliedWalLsn"),
                Vldtn.requireNonNull(stateView, "stateView"),
                newSnapshotProjection(conf),
                Vldtn.requireNonNull(clock, "clock"));
    }

    @Override
    public SegmentIndexRuntimeSnapshot snapshot() {
        final Instant capturedAt = clock.instant();
        final SegmentRuntimeMetrics stableSegmentRuntime =
                stableSegmentRuntimeCollector.collect();
        final ExecutorRegistryStats executorSnapshot =
                executorRegistry.statsSnapshot();
        final MaintenanceStatsSnapshot maintenanceStats =
                maintenanceStatsRecorder.statsSnapshot();
        final RuntimeMonitoringData collected =
                new RuntimeMonitoringData(capturedAt,
                        indexOperationStatsRecorder.statsSnapshot(),
                        segmentRegistry.metricsSnapshot(), chunkStoreCache.stats(),
                        stableSegmentRuntime, executorSnapshot,
                        splitService.statsSnapshot(),
                        walMonitoringView.statsSnapshot(), maintenanceStats,
                        resolveRequestCount(
                                maintenanceStats.getCompactRequestCount(),
                                compactRequestHighWaterMark,
                                stableSegmentRuntime
                                        .getTotalCompactRequestCount()),
                        resolveRequestCount(
                                maintenanceStats.getFlushRequestCount(),
                                flushRequestHighWaterMark,
                                stableSegmentRuntime.getTotalFlushRequestCount()),
                        lastAppliedWalLsn.get(), runtimeTuningState.cacheKeyLimit(),
                        runtimeTuningState.segmentWriteCacheKeyLimit(),
                        runtimeTuningState
                                .segmentWriteCacheKeyLimitDuringMaintenance(),
                        runtimeTuningState.indexBufferedWriteKeyLimit(),
                        stateView.currentState());
        return snapshotProjection.project(collected);
    }

    private static <K, V> IndexRuntimeSnapshotProjection<K, V> newSnapshotProjection(
            final EffectiveIndexConfiguration<K, V> conf) {
        return new IndexRuntimeSnapshotProjection<>(
                Vldtn.requireNonNull(conf, "conf"));
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
