package org.hestiastore.index.segmentindex.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeSnapshot;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;

/**
 * Collects immutable metrics snapshots for the index runtime.
 */
@SuppressWarnings("java:S107")
final class SegmentIndexMetricsCollector<K, V> {

    private final IndexConfiguration<K, V> conf;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final IndexExecutorRegistry executorRegistry;
    private final RuntimeTuningState runtimeTuningState;
    private final WalRuntime<K, V> walRuntime;
    private final Stats stats;
    private final AtomicLong compactRequestHighWaterMark;
    private final AtomicLong flushRequestHighWaterMark;
    private final AtomicLong lastAppliedWalLsn;
    private final Supplier<SegmentIndexState> stateSupplier;

    SegmentIndexMetricsCollector(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final PartitionRuntime<K, V> partitionRuntime,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final IndexExecutorRegistry executorRegistry,
            final RuntimeTuningState runtimeTuningState,
            final WalRuntime<K, V> walRuntime, final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.compactRequestHighWaterMark = Vldtn.requireNonNull(
                compactRequestHighWaterMark, "compactRequestHighWaterMark");
        this.flushRequestHighWaterMark = Vldtn.requireNonNull(
                flushRequestHighWaterMark, "flushRequestHighWaterMark");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
    }

    SegmentIndexMetricsSnapshot metricsSnapshot() {
        final SegmentRegistryCacheStats cacheStats = segmentRegistry
                .metricsSnapshot();
        final StableSegmentRuntimeAggregate stableSegmentRuntime =
                collectStableSegmentRuntime();
        final PartitionRuntimeSnapshot partitionSnapshot = partitionRuntime
                .snapshot();
        final IndexExecutorRegistry.RuntimeSnapshot executorSnapshot =
                executorRegistry.runtimeSnapshot();
        final IndexExecutorRegistry.ExecutorMetricsSnapshot indexMaintenanceExecutor =
                executorSnapshot.getIndexMaintenance();
        final IndexExecutorRegistry.ExecutorMetricsSnapshot splitMaintenanceExecutor =
                executorSnapshot.getSplitMaintenance();
        final IndexExecutorRegistry.ExecutorMetricsSnapshot stableSegmentMaintenanceExecutor =
                executorSnapshot.getStableSegmentMaintenance();
        final var walStats = walRuntime.statsSnapshot();
        final long compactRequestCount = Math.max(stats.getCompactRequestCx(),
                updateHighWaterMark(compactRequestHighWaterMark,
                        stableSegmentRuntime.totalCompactRequestCount));
        final long flushRequestCount = Math.max(stats.getFlushRequestCx(),
                updateHighWaterMark(flushRequestHighWaterMark,
                        stableSegmentRuntime.totalFlushRequestCount));
        return new SegmentIndexMetricsSnapshot(stats.getGetCx(),
                stats.getPutCx(), stats.getDeleteCx(), cacheStats.hitCount(),
                cacheStats.missCount(), cacheStats.loadCount(),
                cacheStats.evictionCount(), cacheStats.size(),
                cacheStats.limit(),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER),
                stableSegmentRuntime.totalMappedStableSegmentCount,
                stableSegmentRuntime.readyStableSegmentCount,
                stableSegmentRuntime.stableSegmentsInMaintenanceStateCount,
                stableSegmentRuntime.errorStableSegmentCount,
                stableSegmentRuntime.closedStableSegmentCount,
                stableSegmentRuntime.unloadedMappedStableSegmentCount,
                stableSegmentRuntime.totalStableSegmentKeyCount,
                stableSegmentRuntime.totalStableSegmentCacheKeyCount,
                stableSegmentRuntime.totalStableSegmentWriteBufferKeyCount
                        + partitionSnapshot.getBufferedKeyCount(),
                stableSegmentRuntime.totalStableSegmentDeltaCacheFileCount,
                compactRequestCount, flushRequestCount, stats.getSplitScheduleCx(),
                backgroundSplitCoordinator.splitInFlightCount(),
                indexMaintenanceExecutor.getQueueSize(),
                indexMaintenanceExecutor.getQueueCapacity(),
                splitMaintenanceExecutor.getQueueSize(),
                splitMaintenanceExecutor.getQueueCapacity(),
                indexMaintenanceExecutor.getActiveThreadCount(),
                indexMaintenanceExecutor.getCompletedTaskCount(),
                indexMaintenanceExecutor.getRejectedTaskCount(),
                splitMaintenanceExecutor.getActiveThreadCount(),
                splitMaintenanceExecutor.getCompletedTaskCount(),
                splitMaintenanceExecutor.getRejectedTaskCount(),
                stableSegmentMaintenanceExecutor.getActiveThreadCount(),
                stableSegmentMaintenanceExecutor.getQueueSize(),
                stableSegmentMaintenanceExecutor.getQueueCapacity(),
                stableSegmentMaintenanceExecutor.getCompletedTaskCount(),
                stableSegmentMaintenanceExecutor.getCallerRunsCount(),
                stats.getReadLatencyP50Micros(),
                stats.getReadLatencyP95Micros(),
                stats.getReadLatencyP99Micros(),
                stats.getWriteLatencyP50Micros(),
                stats.getWriteLatencyP95Micros(),
                stats.getWriteLatencyP99Micros(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                stableSegmentRuntime.totalBloomFilterRequestCount,
                stableSegmentRuntime.totalBloomFilterRefusedCount,
                stableSegmentRuntime.totalBloomFilterPositiveCount,
                stableSegmentRuntime.totalBloomFilterFalsePositiveCount,
                walRuntime.isEnabled(), walStats.appendCount(),
                walStats.appendBytes(), walStats.syncCount(),
                walStats.syncFailureCount(), walStats.corruptionCount(),
                walStats.truncationCount(), walStats.retainedBytes(),
                walStats.segmentCount(), walStats.durableLsn(),
                walStats.checkpointLsn(), walStats.pendingSyncBytes(),
                lastAppliedWalLsn.get(), walStats.syncTotalNanos(),
                walStats.syncMaxNanos(), walStats.syncBatchBytesTotal(),
                walStats.syncBatchBytesMax(),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER),
                partitionSnapshot.getPartitionCount(),
                partitionSnapshot.getActivePartitionCount(),
                partitionSnapshot.getDrainingPartitionCount(),
                partitionSnapshot.getImmutableRunCount(),
                partitionSnapshot.getBufferedKeyCount(),
                partitionSnapshot.getLocalThrottleCount(),
                partitionSnapshot.getGlobalThrottleCount(),
                partitionSnapshot.getDrainScheduleCount(),
                partitionSnapshot.getDrainInFlightCount(),
                stats.getDrainLatencyP95Micros(),
                stats.getSplitTaskStartDelayP95Micros(),
                stats.getSplitTaskRunLatencyP95Micros(),
                stats.getDrainTaskStartDelayP95Micros(),
                stats.getDrainTaskRunLatencyP95Micros(),
                partitionSnapshot.getSplitBlockedPartitionCount(),
                partitionSnapshot.getSplitBlockedDrainScheduleCount(),
                partitionSnapshot.getBufferFullWhileSplitBlockedCount(),
                stats.getPutBusyRetryCx(), stats.getPutBusyTimeoutCx(),
                stats.getPutBusyWaitP95Micros(),
                stats.getFlushAcceptedToReadyP95Micros(),
                stats.getCompactAcceptedToReadyP95Micros(),
                stats.getFlushBusyRetryCx(),
                stats.getCompactBusyRetryCx(),
                stableSegmentRuntime.stableSegmentMetricsSnapshots,
                stateSupplier.get());
    }

    private StableSegmentRuntimeAggregate collectStableSegmentRuntime() {
        final StableSegmentRuntimeAggregate aggregate =
                new StableSegmentRuntimeAggregate();
        final List<SegmentId> mappedSegmentIds = keyToSegmentMap
                .getSegmentIds();
        aggregate.totalMappedStableSegmentCount = mappedSegmentIds.size();
        if (mappedSegmentIds.isEmpty()) {
            return aggregate;
        }
        final Set<SegmentId> mappedSegmentIdSet = new HashSet<>(
                mappedSegmentIds);
        int accountedSegments = 0;
        for (final Segment<K, V> segment : segmentRegistry
                .loadedSegmentsSnapshot()) {
            if (segment != null) {
                final SegmentRuntimeSnapshot segmentRuntime = segment
                        .getRuntimeSnapshot();
                final SegmentId segmentId = segmentRuntime.getSegmentId();
                if (mappedSegmentIdSet.contains(segmentId)) {
                    accountedSegments++;
                    accumulateSegmentMetrics(aggregate, segmentRuntime);
                }
            }
        }
        aggregate.unloadedMappedStableSegmentCount = Math.max(0,
                aggregate.totalMappedStableSegmentCount - accountedSegments);
        return aggregate;
    }

    private static void accumulateSegmentMetrics(
            final StableSegmentRuntimeAggregate aggregate,
            final SegmentRuntimeSnapshot segmentRuntime) {
        final SegmentState state = segmentRuntime.getState();
        if (state == SegmentState.READY) {
            aggregate.readyStableSegmentCount++;
        } else if (state == SegmentState.MAINTENANCE_RUNNING
                || state == SegmentState.FREEZE) {
            aggregate.stableSegmentsInMaintenanceStateCount++;
        } else if (state == SegmentState.ERROR) {
            aggregate.errorStableSegmentCount++;
        } else if (state == SegmentState.CLOSED) {
            aggregate.closedStableSegmentCount++;
        }
        aggregate.totalStableSegmentKeyCount += Math.max(0L,
                segmentRuntime.getNumberOfKeys());
        aggregate.totalStableSegmentCacheKeyCount += Math.max(0L,
                segmentRuntime.getNumberOfKeysInSegmentCache());
        aggregate.totalStableSegmentWriteBufferKeyCount += Math.max(0L,
                segmentRuntime.getNumberOfKeysInWriteCache());
        aggregate.totalStableSegmentDeltaCacheFileCount += Math.max(0,
                segmentRuntime.getNumberOfDeltaCacheFiles());
        aggregate.stableSegmentMetricsSnapshots
                .add(new SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot(
                        segmentRuntime));
        aggregate.totalCompactRequestCount += Math.max(0L,
                segmentRuntime.getNumberOfCompacts());
        aggregate.totalFlushRequestCount += Math.max(0L,
                segmentRuntime.getNumberOfFlushes());
        aggregate.totalBloomFilterRequestCount += Math.max(0L,
                segmentRuntime.getBloomFilterRequestCount());
        aggregate.totalBloomFilterRefusedCount += Math.max(0L,
                segmentRuntime.getBloomFilterRefusedCount());
        aggregate.totalBloomFilterPositiveCount += Math.max(0L,
                segmentRuntime.getBloomFilterPositiveCount());
        aggregate.totalBloomFilterFalsePositiveCount += Math.max(0L,
                segmentRuntime.getBloomFilterFalsePositiveCount());
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

    private static final class StableSegmentRuntimeAggregate {
        private int totalMappedStableSegmentCount;
        private int readyStableSegmentCount;
        private int stableSegmentsInMaintenanceStateCount;
        private int errorStableSegmentCount;
        private int closedStableSegmentCount;
        private int unloadedMappedStableSegmentCount;
        private long totalStableSegmentKeyCount;
        private long totalStableSegmentCacheKeyCount;
        private long totalStableSegmentWriteBufferKeyCount;
        private long totalStableSegmentDeltaCacheFileCount;
        private long totalCompactRequestCount;
        private long totalFlushRequestCount;
        private long totalBloomFilterRequestCount;
        private long totalBloomFilterRefusedCount;
        private long totalBloomFilterPositiveCount;
        private long totalBloomFilterFalsePositiveCount;
        private final List<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot> stableSegmentMetricsSnapshots = new ArrayList<>();
    }
}
