package org.hestiastore.index.segmentindex.runtimemonitoring;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryStats;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsView;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.wal.WalMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;

/**
 * Creates immutable runtime snapshots from collected runtime observations.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexRuntimeSnapshotFactory<K, V> {

    private final EffectiveIndexConfiguration<K, V> conf;
    private final SplitStatsView splitStatsView;
    private final ChunkStoreCache<K, V> chunkStoreCache;
    private final RuntimeTuningState runtimeTuningState;
    private final IndexOperationStatsRecorder indexOperationStatsRecorder;
    private final AtomicLong lastAppliedWalLsn;
    private final SegmentIndexStateView stateView;
    private final IndexRuntimeSnapshotProjection<K, V> projection;

    IndexRuntimeSnapshotFactory(
            final EffectiveIndexConfiguration<K, V> conf,
            final SplitStatsView splitStatsView,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final RuntimeTuningState runtimeTuningState,
            final IndexOperationStatsRecorder indexOperationStatsRecorder,
            final AtomicLong lastAppliedWalLsn,
            final SegmentIndexStateView stateView) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.splitStatsView = Vldtn.requireNonNull(splitStatsView,
                "splitStatsView");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.indexOperationStatsRecorder = Vldtn.requireNonNull(
                indexOperationStatsRecorder, "indexOperationStatsRecorder");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.stateView = Vldtn.requireNonNull(stateView, "stateView");
        this.projection = new IndexRuntimeSnapshotProjection<>(this.conf);
    }

    IndexRuntimeSnapshot create(
            final Instant capturedAt,
            final SegmentRegistryCacheStats cacheStats,
            final StableSegmentRuntimeMetrics stableSegmentRuntime,
            final ExecutorRegistryStats executorSnapshot,
            final WalMonitoring walMonitoring,
            final MaintenanceStats maintenanceStats,
            final long compactRequestCount,
            final long flushRequestCount) {
        final MaintenanceStats resolvedMaintenanceStats =
                Vldtn.requireNonNull(maintenanceStats, "maintenanceStats");
        final Instant resolvedCapturedAt = Vldtn.requireNonNull(capturedAt,
                "capturedAt");
        final CollectedRuntimeMonitoringData collected =
                new CollectedRuntimeMonitoringData(
                        resolvedCapturedAt,
                        indexOperationStatsRecorder.statsSnapshot(), cacheStats,
                        chunkStoreCache.stats(), stableSegmentRuntime,
                        executorSnapshot, splitStatsView.statsSnapshot(),
                        walMonitoring,
                        resolvedMaintenanceStats, compactRequestCount,
                        flushRequestCount, lastAppliedWalLsn.get(),
                        runtimeTuningState.cacheKeyLimit(),
                        runtimeTuningState.segmentWriteCacheKeyLimit(),
                        runtimeTuningState
                                .segmentWriteCacheKeyLimitDuringMaintenance(),
                        runtimeTuningState.indexBufferedWriteKeyLimit(),
                        stateView.currentState());
        return projection.project(collected);
    }
}
