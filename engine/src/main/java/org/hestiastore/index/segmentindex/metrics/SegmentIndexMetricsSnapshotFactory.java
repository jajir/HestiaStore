package org.hestiastore.index.segmentindex.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCacheStats;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryStats;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorStats;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.wal.WalStats;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;

/**
 * Creates immutable metrics snapshots from collected runtime observations.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexMetricsSnapshotFactory<K, V> {

    private final EffectiveIndexConfiguration<K, V> conf;
    private final Supplier<SplitStats> splitStatsSupplier;
    private final Supplier<ChunkStoreCacheStats> chunkStoreCacheStatsSupplier;
    private final RuntimeTuningState runtimeTuningState;
    private final Supplier<IndexOperationStats> indexOperationStatsSupplier;
    private final AtomicLong lastAppliedWalLsn;
    private final Supplier<SegmentIndexState> stateSupplier;

    SegmentIndexMetricsSnapshotFactory(
            final EffectiveIndexConfiguration<K, V> conf,
            final Supplier<SplitStats> splitStatsSupplier,
            final Supplier<ChunkStoreCacheStats> chunkStoreCacheStatsSupplier,
            final RuntimeTuningState runtimeTuningState,
            final Supplier<IndexOperationStats> indexOperationStatsSupplier,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.splitStatsSupplier = Vldtn.requireNonNull(
                splitStatsSupplier, "splitStatsSupplier");
        this.chunkStoreCacheStatsSupplier = Vldtn.requireNonNull(
                chunkStoreCacheStatsSupplier, "chunkStoreCacheStatsSupplier");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.indexOperationStatsSupplier = Vldtn.requireNonNull(
                indexOperationStatsSupplier, "indexOperationStatsSupplier");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
    }

    SegmentIndexMetricsSnapshot create(
            final SegmentRegistryCacheStats cacheStats,
            final StableSegmentRuntimeMetrics stableSegmentRuntime,
            final ExecutorRegistryStats executorSnapshot,
            final WalStats walStats, final MaintenanceStats maintenanceStats,
            final long compactRequestCount,
            final long flushRequestCount) {
        final SplitStats splitStats = splitStatsSupplier.get();
        final ChunkStoreCacheStats chunkStoreCacheStats =
                chunkStoreCacheStatsSupplier.get();
        final IndexOperationStats operationStats =
                indexOperationStatsSupplier.get();
        final MaintenanceStats resolvedMaintenanceStats =
                Vldtn.requireNonNull(maintenanceStats, "maintenanceStats");
        return new SegmentIndexMetricsSnapshot(operationStats.getGetCount(),
                operationStats.getPutCount(),
                operationStats.getDeleteCount(), cacheStats.hitCount(),
                cacheStats.missCount(), cacheStats.loadCount(),
                cacheStats.evictionCount(), cacheStats.size(),
                cacheStats.limit(),
                chunkStoreCacheStats.pageLimit(),
                chunkStoreCacheStats.pageCount(),
                chunkStoreCacheStats.entryCount(),
                chunkStoreCacheStats.hitCount(),
                chunkStoreCacheStats.missCount(),
                chunkStoreCacheStats.loadCount(),
                chunkStoreCacheStats.evictionCount(),
                chunkStoreCacheStats.invalidationCount(),
                runtimeTuningState.cacheKeyLimit(),
                runtimeTuningState.segmentWriteCacheKeyLimit(),
                runtimeTuningState
                        .segmentWriteCacheKeyLimitDuringMaintenance(),
                runtimeTuningState.indexBufferedWriteKeyLimit(),
                stableSegmentRuntime.getTotalMappedStableSegmentCount(),
                stableSegmentRuntime.getReadyStableSegmentCount(),
                stableSegmentRuntime
                        .getStableSegmentsInMaintenanceStateCount(),
                stableSegmentRuntime.getErrorStableSegmentCount(),
                stableSegmentRuntime.getClosedStableSegmentCount(),
                stableSegmentRuntime.getUnloadedMappedStableSegmentCount(),
                stableSegmentRuntime.getTotalStableSegmentKeyCount(),
                stableSegmentRuntime.getTotalStableSegmentCacheKeyCount(),
                stableSegmentRuntime.getTotalStableSegmentWriteBufferKeyCount(),
                stableSegmentRuntime.getTotalStableSegmentDeltaCacheFileCount(),
                compactRequestCount, flushRequestCount,
                splitStats.splitScheduleCount(),
                splitStats.splitInFlightCount(),
                indexMaintenanceSnapshot(executorSnapshot).getQueueSize(),
                indexMaintenanceSnapshot(executorSnapshot).getQueueCapacity(),
                splitMaintenanceSnapshot(executorSnapshot).getQueueSize(),
                splitMaintenanceSnapshot(executorSnapshot).getQueueCapacity(),
                indexMaintenanceSnapshot(executorSnapshot)
                        .getActiveThreadCount(),
                indexMaintenanceSnapshot(executorSnapshot)
                        .getCompletedTaskCount(),
                indexMaintenanceSnapshot(executorSnapshot)
                        .getRejectedTaskCount(),
                splitMaintenanceSnapshot(executorSnapshot)
                        .getActiveThreadCount(),
                splitMaintenanceSnapshot(executorSnapshot)
                        .getCompletedTaskCount(),
                splitMaintenanceSnapshot(executorSnapshot)
                        .getRejectedTaskCount(),
                stableSegmentMaintenanceSnapshot(executorSnapshot)
                        .getActiveThreadCount(),
                stableSegmentMaintenanceSnapshot(executorSnapshot)
                        .getQueueSize(),
                stableSegmentMaintenanceSnapshot(executorSnapshot)
                        .getQueueCapacity(),
                stableSegmentMaintenanceSnapshot(executorSnapshot)
                        .getCompletedTaskCount(),
                stableSegmentMaintenanceSnapshot(executorSnapshot)
                        .getCallerRunsCount(),
                operationStats.getReadLatencyP50Micros(),
                operationStats.getReadLatencyP95Micros(),
                operationStats.getReadLatencyP99Micros(),
                operationStats.getWriteLatencyP50Micros(),
                operationStats.getWriteLatencyP95Micros(),
                operationStats.getWriteLatencyP99Micros(),
                conf.bloomFilter().hashFunctions(),
                conf.bloomFilter().indexSizeBytes(),
                conf.bloomFilter().falsePositiveProbability(),
                stableSegmentRuntime.getTotalBloomFilterRequestCount(),
                stableSegmentRuntime.getTotalBloomFilterRefusedCount(),
                stableSegmentRuntime.getTotalBloomFilterPositiveCount(),
                stableSegmentRuntime.getTotalBloomFilterFalsePositiveCount(),
                conf.wal().isEnabled(), walStats.appendCount(),
                walStats.appendBytes(), walStats.syncCount(),
                walStats.syncFailureCount(), walStats.corruptionCount(),
                walStats.truncationCount(), walStats.retainedBytes(),
                walStats.segmentCount(), walStats.durableLsn(),
                walStats.checkpointLsn(), walStats.pendingSyncBytes(),
                lastAppliedWalLsn.get(), walStats.syncTotalNanos(),
                walStats.syncMaxNanos(), walStats.syncBatchBytesTotal(),
                walStats.syncBatchBytesMax(),
                splitStats.splitTaskStartDelayP95Micros(),
                splitStats.splitTaskRunLatencyP95Micros(),
                0L,
                0L,
                0L, 0L, 0L,
                resolvedMaintenanceStats.getFlushAcceptedToReadyP95Micros(),
                resolvedMaintenanceStats.getCompactAcceptedToReadyP95Micros(),
                resolvedMaintenanceStats.getFlushBusyRetryCount(),
                resolvedMaintenanceStats.getCompactBusyRetryCount(),
                stableSegmentRuntime.getStableSegmentMetricsSnapshots(),
                stateSupplier.get());
    }

    private static ExecutorStats indexMaintenanceSnapshot(
            final ExecutorRegistryStats executorSnapshot) {
        return executorSnapshot.getIndexMaintenance();
    }

    private static ExecutorStats splitMaintenanceSnapshot(
            final ExecutorRegistryStats executorSnapshot) {
        return executorSnapshot.getSplitMaintenance();
    }

    private static ExecutorStats stableSegmentMaintenanceSnapshot(
            final ExecutorRegistryStats executorSnapshot) {
        return executorSnapshot.getStableSegmentMaintenance();
    }
}
