package org.hestiastore.index.segmentindex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentindex.wal.WalStats;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;

/**
 * Creates immutable metrics snapshots from collected runtime observations.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexMetricsSnapshotFactory<K, V> {

    private final IndexConfiguration<K, V> conf;
    private final Supplier<SplitMetricsSnapshot> splitSnapshotSupplier;
    private final RuntimeTuningState runtimeTuningState;
    private final WalRuntime<K, V> walRuntime;
    private final Stats stats;
    private final AtomicLong lastAppliedWalLsn;
    private final Supplier<SegmentIndexState> stateSupplier;

    SegmentIndexMetricsSnapshotFactory(final IndexConfiguration<K, V> conf,
            final Supplier<SplitMetricsSnapshot> splitSnapshotSupplier,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.splitSnapshotSupplier = Vldtn.requireNonNull(
                splitSnapshotSupplier, "splitSnapshotSupplier");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
    }

    SegmentIndexMetricsSnapshot create(
            final SegmentRegistryCacheStats cacheStats,
            final StableSegmentRuntimeMetrics stableSegmentRuntime,
            final IndexExecutorRuntimeAccess executorSnapshot,
            final WalStats walStats, final long compactRequestCount,
            final long flushRequestCount) {
        final SplitMetricsSnapshot splitSnapshot = splitSnapshotSupplier.get();
        return new SegmentIndexMetricsSnapshot(stats.getGetCount(),
                stats.getPutCount(),
                stats.getDeleteCount(), cacheStats.hitCount(),
                cacheStats.missCount(), cacheStats.loadCount(),
                cacheStats.evictionCount(), cacheStats.size(),
                cacheStats.limit(), effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE),
                effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION),
                effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER),
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
                stats.getSplitScheduleCount(),
                splitSnapshot.splitInFlightCount(),
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
                stats.getReadLatencyP50Micros(),
                stats.getReadLatencyP95Micros(),
                stats.getReadLatencyP99Micros(),
                stats.getWriteLatencyP50Micros(),
                stats.getWriteLatencyP95Micros(),
                stats.getWriteLatencyP99Micros(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                stableSegmentRuntime.getTotalBloomFilterRequestCount(),
                stableSegmentRuntime.getTotalBloomFilterRefusedCount(),
                stableSegmentRuntime.getTotalBloomFilterPositiveCount(),
                stableSegmentRuntime.getTotalBloomFilterFalsePositiveCount(),
                walRuntime.isEnabled(), walStats.appendCount(),
                walStats.appendBytes(), walStats.syncCount(),
                walStats.syncFailureCount(), walStats.corruptionCount(),
                walStats.truncationCount(), walStats.retainedBytes(),
                walStats.segmentCount(), walStats.durableLsn(),
                walStats.checkpointLsn(), walStats.pendingSyncBytes(),
                lastAppliedWalLsn.get(), walStats.syncTotalNanos(),
                walStats.syncMaxNanos(), walStats.syncBatchBytesTotal(),
                walStats.syncBatchBytesMax(),
                effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION),
                effectiveValue(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER),
                0, 0, 0, 0, 0, 0L, 0L, 0L, 0,
                stats.getDrainLatencyP95Micros(),
                stats.getSplitTaskStartDelayP95Micros(),
                stats.getSplitTaskRunLatencyP95Micros(),
                stats.getDrainTaskStartDelayP95Micros(),
                stats.getDrainTaskRunLatencyP95Micros(),
                splitSnapshot.splitBlockedCount(), 0L, 0L,
                stats.getPutBusyRetryCount(),
                stats.getPutBusyTimeoutCount(),
                stats.getPutBusyWaitP95Micros(),
                stats.getFlushAcceptedToReadyP95Micros(),
                stats.getCompactAcceptedToReadyP95Micros(),
                stats.getFlushBusyRetryCount(),
                stats.getCompactBusyRetryCount(),
                stableSegmentRuntime.getStableSegmentMetricsSnapshots(),
                stateSupplier.get());
    }

    private int effectiveValue(final RuntimeSettingKey key) {
        return runtimeTuningState.effectiveValue(key);
    }

    private static IndexExecutorMetricsAccess indexMaintenanceSnapshot(
            final IndexExecutorRuntimeAccess executorSnapshot) {
        return executorSnapshot.getIndexMaintenance();
    }

    private static IndexExecutorMetricsAccess splitMaintenanceSnapshot(
            final IndexExecutorRuntimeAccess executorSnapshot) {
        return executorSnapshot.getSplitMaintenance();
    }

    private static IndexExecutorMetricsAccess stableSegmentMaintenanceSnapshot(
            final IndexExecutorRuntimeAccess executorSnapshot) {
        return executorSnapshot.getStableSegmentMaintenance();
    }
}
