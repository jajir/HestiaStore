package org.hestiastore.index.segmentindex.runtimemonitoring;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorStats;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexBloomFilterMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexChunkStoreCacheMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexExecutorMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexLatencyMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexMaintenanceMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexOperationMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexRegistryCacheMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexSegmentMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexSplitMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexWalMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexWritePathMetrics;

/**
 * Projects internal collected monitoring data into the user-facing runtime
 * monitoring model.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexRuntimeSnapshotProjection<K, V> {

    private final EffectiveIndexConfiguration<K, V> conf;

    IndexRuntimeSnapshotProjection(
            final EffectiveIndexConfiguration<K, V> conf) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
    }

    IndexRuntimeSnapshot project(
            final CollectedRuntimeMonitoringData collected) {
        final CollectedRuntimeMonitoringData metrics = Vldtn.requireNonNull(
                collected, "collected");
        return new IndexRuntimeSnapshot(
                conf.identity().name(),
                metrics.state(),
                metrics.capturedAt(),
                operations(metrics),
                registryCache(metrics),
                chunkStoreCache(metrics),
                segments(metrics),
                writePath(metrics),
                maintenance(metrics),
                split(metrics),
                latency(metrics),
                bloomFilter(metrics),
                wal(metrics));
    }

    private SegmentIndexOperationMetrics operations(
            final CollectedRuntimeMonitoringData metrics) {
        return new SegmentIndexOperationMetrics(
                metrics.operationStats().getGetCount(),
                metrics.operationStats().getPutCount(),
                metrics.operationStats().getDeleteCount());
    }

    private SegmentIndexRegistryCacheMetrics registryCache(
            final CollectedRuntimeMonitoringData metrics) {
        return new SegmentIndexRegistryCacheMetrics(
                metrics.registryCacheStats().hitCount(),
                metrics.registryCacheStats().missCount(),
                metrics.registryCacheStats().loadCount(),
                metrics.registryCacheStats().evictionCount(),
                metrics.registryCacheStats().size(),
                metrics.registryCacheStats().limit());
    }

    private SegmentIndexChunkStoreCacheMetrics chunkStoreCache(
            final CollectedRuntimeMonitoringData metrics) {
        return new SegmentIndexChunkStoreCacheMetrics(
                metrics.chunkStoreCacheStats().pageLimit(),
                metrics.chunkStoreCacheStats().pageCount(),
                metrics.chunkStoreCacheStats().entryCount(),
                metrics.chunkStoreCacheStats().hitCount(),
                metrics.chunkStoreCacheStats().missCount(),
                metrics.chunkStoreCacheStats().loadCount(),
                metrics.chunkStoreCacheStats().evictionCount(),
                metrics.chunkStoreCacheStats().invalidationCount());
    }

    private SegmentIndexSegmentMetrics segments(
            final CollectedRuntimeMonitoringData metrics) {
        final StableSegmentRuntimeMetrics stable =
                metrics.stableSegmentRuntime();
        return new SegmentIndexSegmentMetrics(
                metrics.segmentCacheKeyLimit(),
                stable.getTotalMappedStableSegmentCount(),
                stable.getReadyStableSegmentCount(),
                stable.getStableSegmentsInMaintenanceStateCount(),
                stable.getErrorStableSegmentCount(),
                stable.getClosedStableSegmentCount(),
                stable.getUnloadedMappedStableSegmentCount(),
                stable.getTotalStableSegmentKeyCount(),
                stable.getTotalStableSegmentCacheKeyCount(),
                stable.getTotalStableSegmentDeltaCacheFileCount(),
                stable.getStableSegmentMetricsSnapshots());
    }

    private SegmentIndexWritePathMetrics writePath(
            final CollectedRuntimeMonitoringData metrics) {
        final StableSegmentRuntimeMetrics stable =
                metrics.stableSegmentRuntime();
        return new SegmentIndexWritePathMetrics(
                metrics.segmentWriteCacheKeyLimit(),
                metrics.segmentWriteCacheKeyLimitDuringMaintenance(),
                metrics.indexBufferedWriteKeyLimit(),
                stable.getTotalStableSegmentWriteBufferKeyCount());
    }

    private SegmentIndexMaintenanceMetrics maintenance(
            final CollectedRuntimeMonitoringData metrics) {
        return new SegmentIndexMaintenanceMetrics(
                metrics.compactRequestCount(),
                metrics.flushRequestCount(),
                metrics.maintenanceStats().getFlushAcceptedToReadyP95Micros(),
                metrics.maintenanceStats()
                        .getCompactAcceptedToReadyP95Micros(),
                metrics.maintenanceStats().getFlushBusyRetryCount(),
                metrics.maintenanceStats().getCompactBusyRetryCount(),
                executor(metrics.executorStats().getIndexMaintenance()),
                executor(metrics.executorStats().getStableSegmentMaintenance()));
    }

    private SegmentIndexSplitMetrics split(
            final CollectedRuntimeMonitoringData metrics) {
        return new SegmentIndexSplitMetrics(
                metrics.splitStats().splitScheduleCount(),
                metrics.splitStats().splitInFlightCount(),
                metrics.splitStats().splitBlockedCount(),
                metrics.splitStats().splitTaskStartDelayP95Micros(),
                metrics.splitStats().splitTaskRunLatencyP95Micros(),
                executor(metrics.executorStats().getSplitMaintenance()));
    }

    private SegmentIndexLatencyMetrics latency(
            final CollectedRuntimeMonitoringData metrics) {
        return new SegmentIndexLatencyMetrics(
                metrics.operationStats().getReadLatencyP50Micros(),
                metrics.operationStats().getReadLatencyP95Micros(),
                metrics.operationStats().getReadLatencyP99Micros(),
                metrics.operationStats().getWriteLatencyP50Micros(),
                metrics.operationStats().getWriteLatencyP95Micros(),
                metrics.operationStats().getWriteLatencyP99Micros());
    }

    private SegmentIndexBloomFilterMetrics bloomFilter(
            final CollectedRuntimeMonitoringData metrics) {
        final StableSegmentRuntimeMetrics stable =
                metrics.stableSegmentRuntime();
        return new SegmentIndexBloomFilterMetrics(
                conf.bloomFilter().hashFunctions(),
                conf.bloomFilter().indexSizeBytes(),
                conf.bloomFilter().falsePositiveProbability(),
                stable.getTotalBloomFilterRequestCount(),
                stable.getTotalBloomFilterRefusedCount(),
                stable.getTotalBloomFilterPositiveCount(),
                stable.getTotalBloomFilterFalsePositiveCount());
    }

    private SegmentIndexWalMetrics wal(
            final CollectedRuntimeMonitoringData metrics) {
        return new SegmentIndexWalMetrics(
                conf.wal().isEnabled(),
                metrics.walMonitoring().appendCount(),
                metrics.walMonitoring().appendBytes(),
                metrics.walMonitoring().syncCount(),
                metrics.walMonitoring().syncFailureCount(),
                metrics.walMonitoring().corruptionCount(),
                metrics.walMonitoring().truncationCount(),
                metrics.walMonitoring().retainedBytes(),
                metrics.walMonitoring().segmentCount(),
                metrics.walMonitoring().durableLsn(),
                metrics.walMonitoring().checkpointLsn(),
                metrics.walMonitoring().pendingSyncBytes(),
                metrics.appliedWalLsn(),
                metrics.walMonitoring().syncTotalNanos(),
                metrics.walMonitoring().syncMaxNanos(),
                metrics.walMonitoring().syncBatchBytesTotal(),
                metrics.walMonitoring().syncBatchBytesMax());
    }

    private SegmentIndexExecutorMetrics executor(final ExecutorStats stats) {
        return new SegmentIndexExecutorMetrics(
                stats.getActiveThreadCount(),
                stats.getQueueSize(),
                stats.getQueueCapacity(),
                stats.getCompletedTaskCount(),
                stats.getRejectedTaskCount(),
                stats.getCallerRunsCount());
    }
}
