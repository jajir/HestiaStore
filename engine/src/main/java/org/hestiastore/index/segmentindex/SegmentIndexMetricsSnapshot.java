package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;

/**
 * Immutable snapshot of index and segment runtime metrics.
 */
@SuppressWarnings("java:S107")
public final class SegmentIndexMetricsSnapshot {

    private final long getOperationCount;
    private final long putOperationCount;
    private final long deleteOperationCount;
    private final long registryCacheHitCount;
    private final long registryCacheMissCount;
    private final long registryCacheLoadCount;
    private final long registryCacheEvictionCount;
    private final int registryCacheSize;
    private final int registryCacheLimit;
    private final int segmentCacheKeyLimitPerSegment;
    private final int maxNumberOfKeysInActivePartition;
    private final int maxNumberOfKeysInPartitionBuffer;
    private final int maxNumberOfImmutableRunsPerPartition;
    private final int maxNumberOfKeysInIndexBuffer;
    private final int segmentCount;
    private final int segmentReadyCount;
    private final int segmentMaintenanceCount;
    private final int segmentErrorCount;
    private final int segmentClosedCount;
    private final int segmentBusyCount;
    private final long totalSegmentKeys;
    private final long totalSegmentCacheKeys;
    private final long totalBufferedWriteKeys;
    private final long totalDeltaCacheFiles;
    private final long compactRequestCount;
    private final long flushRequestCount;
    private final long splitScheduleCount;
    private final int splitInFlightCount;
    private final int maintenanceQueueSize;
    private final int maintenanceQueueCapacity;
    private final int splitQueueSize;
    private final int splitQueueCapacity;
    private final int partitionCount;
    private final int activePartitionCount;
    private final int drainingPartitionCount;
    private final int immutableRunCount;
    private final int partitionBufferedKeyCount;
    private final long localThrottleCount;
    private final long globalThrottleCount;
    private final long drainScheduleCount;
    private final int drainInFlightCount;
    private final long drainLatencyP95Micros;
    private final long readLatencyP50Micros;
    private final long readLatencyP95Micros;
    private final long readLatencyP99Micros;
    private final long writeLatencyP50Micros;
    private final long writeLatencyP95Micros;
    private final long writeLatencyP99Micros;
    private final int bloomFilterHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final double bloomFilterProbabilityOfFalsePositive;
    private final long bloomFilterRequestCount;
    private final long bloomFilterRefusedCount;
    private final long bloomFilterPositiveCount;
    private final long bloomFilterFalsePositiveCount;
    private final boolean walEnabled;
    private final long walAppendCount;
    private final long walAppendBytes;
    private final long walSyncCount;
    private final long walSyncFailureCount;
    private final long walCorruptionCount;
    private final long walTruncationCount;
    private final long walRetainedBytes;
    private final int walSegmentCount;
    private final long walDurableLsn;
    private final long walCheckpointLsn;
    private final long walPendingSyncBytes;
    private final long walAppliedLsn;
    private final long walSyncTotalNanos;
    private final long walSyncMaxNanos;
    private final long walSyncBatchBytesTotal;
    private final long walSyncBatchBytesMax;
    private final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots;
    private final SegmentIndexState state;

    /**
     * Full runtime metrics constructor including per-segment metrics.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int segmentCount, final int segmentReadyCount,
            final int segmentMaintenanceCount, final int segmentErrorCount,
            final int segmentClosedCount, final int segmentBusyCount,
            final long totalSegmentKeys, final long totalSegmentCacheKeys,
            final long totalBufferedWriteKeys, final long totalDeltaCacheFiles,
            final long compactRequestCount, final long flushRequestCount,
            final long splitScheduleCount, final int splitInFlightCount,
            final int maintenanceQueueSize, final int maintenanceQueueCapacity,
            final int splitQueueSize, final int splitQueueCapacity,
            final long readLatencyP50Micros, final long readLatencyP95Micros,
            final long readLatencyP99Micros, final long writeLatencyP50Micros,
            final long writeLatencyP95Micros, final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
            final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots,
            final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount,
                registryCacheHitCount, registryCacheMissCount,
                registryCacheLoadCount, registryCacheEvictionCount,
                registryCacheSize, registryCacheLimit,
                segmentCacheKeyLimitPerSegment,
                maxNumberOfKeysInActivePartition,
                maxNumberOfKeysInPartitionBuffer,
                segmentCount, segmentReadyCount, segmentMaintenanceCount,
                segmentErrorCount, segmentClosedCount, segmentBusyCount,
                totalSegmentKeys, totalSegmentCacheKeys, totalBufferedWriteKeys,
                totalDeltaCacheFiles, compactRequestCount, flushRequestCount,
                splitScheduleCount, splitInFlightCount, maintenanceQueueSize,
                maintenanceQueueCapacity, splitQueueSize, splitQueueCapacity,
                readLatencyP50Micros, readLatencyP95Micros,
                readLatencyP99Micros, writeLatencyP50Micros,
                writeLatencyP95Micros, writeLatencyP99Micros,
                bloomFilterHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, bloomFilterRequestCount,
                bloomFilterRefusedCount, bloomFilterPositiveCount,
                bloomFilterFalsePositiveCount, false, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0,
                0L,
                0L, 0L, 0,
                segmentRuntimeSnapshots, state);
    }

    /**
     * Full runtime metrics constructor including per-segment and WAL metrics.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int segmentCount, final int segmentReadyCount,
            final int segmentMaintenanceCount, final int segmentErrorCount,
            final int segmentClosedCount, final int segmentBusyCount,
            final long totalSegmentKeys, final long totalSegmentCacheKeys,
            final long totalBufferedWriteKeys, final long totalDeltaCacheFiles,
            final long compactRequestCount, final long flushRequestCount,
            final long splitScheduleCount, final int splitInFlightCount,
            final int maintenanceQueueSize, final int maintenanceQueueCapacity,
            final int splitQueueSize, final int splitQueueCapacity,
            final long readLatencyP50Micros, final long readLatencyP95Micros,
            final long readLatencyP99Micros, final long writeLatencyP50Micros,
            final long writeLatencyP95Micros, final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
            final boolean walEnabled, final long walAppendCount,
            final long walAppendBytes, final long walSyncCount,
            final long walSyncFailureCount, final long walCorruptionCount,
            final long walTruncationCount, final long walRetainedBytes,
            final int walSegmentCount, final long walDurableLsn,
            final long walCheckpointLsn, final long walPendingSyncBytes,
            final long walAppliedLsn, final long walSyncTotalNanos,
            final long walSyncMaxNanos, final long walSyncBatchBytesTotal,
            final long walSyncBatchBytesMax,
            final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots,
            final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount,
                registryCacheHitCount, registryCacheMissCount,
                registryCacheLoadCount, registryCacheEvictionCount,
                registryCacheSize, registryCacheLimit,
                segmentCacheKeyLimitPerSegment,
                maxNumberOfKeysInActivePartition,
                maxNumberOfKeysInPartitionBuffer,
                segmentCount, segmentReadyCount, segmentMaintenanceCount,
                segmentErrorCount, segmentClosedCount, segmentBusyCount,
                totalSegmentKeys, totalSegmentCacheKeys, totalBufferedWriteKeys,
                totalDeltaCacheFiles, compactRequestCount, flushRequestCount,
                splitScheduleCount, splitInFlightCount, maintenanceQueueSize,
                maintenanceQueueCapacity, splitQueueSize, splitQueueCapacity,
                readLatencyP50Micros, readLatencyP95Micros,
                readLatencyP99Micros, writeLatencyP50Micros,
                writeLatencyP95Micros, writeLatencyP99Micros,
                bloomFilterHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, bloomFilterRequestCount,
                bloomFilterRefusedCount, bloomFilterPositiveCount,
                bloomFilterFalsePositiveCount, walEnabled, walAppendCount,
                walAppendBytes, walSyncCount, walSyncFailureCount,
                walCorruptionCount, walTruncationCount, walRetainedBytes,
                walSegmentCount, walDurableLsn, walCheckpointLsn,
                walPendingSyncBytes, walAppliedLsn, walSyncTotalNanos,
                walSyncMaxNanos, walSyncBatchBytesTotal,
                walSyncBatchBytesMax, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0,
                segmentRuntimeSnapshots, state);
    }

    /**
     * Full runtime metrics constructor including per-segment and partition
     * metrics, without WAL metrics.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int segmentCount, final int segmentReadyCount,
            final int segmentMaintenanceCount, final int segmentErrorCount,
            final int segmentClosedCount, final int segmentBusyCount,
            final long totalSegmentKeys, final long totalSegmentCacheKeys,
            final long totalBufferedWriteKeys, final long totalDeltaCacheFiles,
            final long compactRequestCount, final long flushRequestCount,
            final long splitScheduleCount, final int splitInFlightCount,
            final int maintenanceQueueSize, final int maintenanceQueueCapacity,
            final int splitQueueSize, final int splitQueueCapacity,
            final long readLatencyP50Micros, final long readLatencyP95Micros,
            final long readLatencyP99Micros, final long writeLatencyP50Micros,
            final long writeLatencyP95Micros, final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
            final int maxNumberOfImmutableRunsPerPartition,
            final int maxNumberOfKeysInIndexBuffer,
            final int partitionCount, final int activePartitionCount,
            final int drainingPartitionCount, final int immutableRunCount,
            final int partitionBufferedKeyCount,
            final long localThrottleCount, final long globalThrottleCount,
            final long drainScheduleCount, final int drainInFlightCount,
            final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots,
            final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount,
                registryCacheHitCount, registryCacheMissCount,
                registryCacheLoadCount, registryCacheEvictionCount,
                registryCacheSize, registryCacheLimit,
                segmentCacheKeyLimitPerSegment,
                maxNumberOfKeysInActivePartition,
                maxNumberOfKeysInPartitionBuffer,
                segmentCount, segmentReadyCount, segmentMaintenanceCount,
                segmentErrorCount, segmentClosedCount, segmentBusyCount,
                totalSegmentKeys, totalSegmentCacheKeys, totalBufferedWriteKeys,
                totalDeltaCacheFiles, compactRequestCount, flushRequestCount,
                splitScheduleCount, splitInFlightCount, maintenanceQueueSize,
                maintenanceQueueCapacity, splitQueueSize, splitQueueCapacity,
                readLatencyP50Micros, readLatencyP95Micros,
                readLatencyP99Micros, writeLatencyP50Micros,
                writeLatencyP95Micros, writeLatencyP99Micros,
                bloomFilterHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, bloomFilterRequestCount,
                bloomFilterRefusedCount, bloomFilterPositiveCount,
                bloomFilterFalsePositiveCount, false, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, maxNumberOfImmutableRunsPerPartition,
                maxNumberOfKeysInIndexBuffer, partitionCount,
                activePartitionCount, drainingPartitionCount,
                immutableRunCount, partitionBufferedKeyCount,
                localThrottleCount, globalThrottleCount, drainScheduleCount,
                drainInFlightCount, 0L, segmentRuntimeSnapshots, state);
    }

    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int segmentCount, final int segmentReadyCount,
            final int segmentMaintenanceCount, final int segmentErrorCount,
            final int segmentClosedCount, final int segmentBusyCount,
            final long totalSegmentKeys, final long totalSegmentCacheKeys,
            final long totalBufferedWriteKeys, final long totalDeltaCacheFiles,
            final long compactRequestCount, final long flushRequestCount,
            final long splitScheduleCount, final int splitInFlightCount,
            final int maintenanceQueueSize, final int maintenanceQueueCapacity,
            final int splitQueueSize, final int splitQueueCapacity,
            final long readLatencyP50Micros, final long readLatencyP95Micros,
            final long readLatencyP99Micros, final long writeLatencyP50Micros,
            final long writeLatencyP95Micros, final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
            final boolean walEnabled, final long walAppendCount,
            final long walAppendBytes, final long walSyncCount,
            final long walSyncFailureCount, final long walCorruptionCount,
            final long walTruncationCount, final long walRetainedBytes,
            final int walSegmentCount, final long walDurableLsn,
            final long walCheckpointLsn, final long walPendingSyncBytes,
            final long walAppliedLsn, final long walSyncTotalNanos,
            final long walSyncMaxNanos, final long walSyncBatchBytesTotal,
            final long walSyncBatchBytesMax,
            final int maxNumberOfImmutableRunsPerPartition,
            final int maxNumberOfKeysInIndexBuffer,
            final int partitionCount, final int activePartitionCount,
            final int drainingPartitionCount, final int immutableRunCount,
            final int partitionBufferedKeyCount,
            final long localThrottleCount, final long globalThrottleCount,
            final long drainScheduleCount, final int drainInFlightCount,
            final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots,
            final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount,
                registryCacheHitCount, registryCacheMissCount,
                registryCacheLoadCount, registryCacheEvictionCount,
                registryCacheSize, registryCacheLimit,
                segmentCacheKeyLimitPerSegment,
                maxNumberOfKeysInActivePartition,
                maxNumberOfKeysInPartitionBuffer,
                segmentCount, segmentReadyCount, segmentMaintenanceCount,
                segmentErrorCount, segmentClosedCount, segmentBusyCount,
                totalSegmentKeys, totalSegmentCacheKeys, totalBufferedWriteKeys,
                totalDeltaCacheFiles, compactRequestCount, flushRequestCount,
                splitScheduleCount, splitInFlightCount, maintenanceQueueSize,
                maintenanceQueueCapacity, splitQueueSize, splitQueueCapacity,
                readLatencyP50Micros, readLatencyP95Micros,
                readLatencyP99Micros, writeLatencyP50Micros,
                writeLatencyP95Micros, writeLatencyP99Micros,
                bloomFilterHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, bloomFilterRequestCount,
                bloomFilterRefusedCount, bloomFilterPositiveCount,
                bloomFilterFalsePositiveCount, walEnabled, walAppendCount,
                walAppendBytes, walSyncCount, walSyncFailureCount,
                walCorruptionCount, walTruncationCount, walRetainedBytes,
                walSegmentCount, walDurableLsn, walCheckpointLsn,
                walPendingSyncBytes, walAppliedLsn, walSyncTotalNanos,
                walSyncMaxNanos, walSyncBatchBytesTotal,
                walSyncBatchBytesMax,
                maxNumberOfImmutableRunsPerPartition,
                maxNumberOfKeysInIndexBuffer, partitionCount,
                activePartitionCount, drainingPartitionCount,
                immutableRunCount, partitionBufferedKeyCount,
                localThrottleCount, globalThrottleCount, drainScheduleCount,
                drainInFlightCount, 0L, segmentRuntimeSnapshots, state);
    }

    /**
     * Full runtime metrics constructor including per-segment and WAL metrics.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int segmentCount, final int segmentReadyCount,
            final int segmentMaintenanceCount, final int segmentErrorCount,
            final int segmentClosedCount, final int segmentBusyCount,
            final long totalSegmentKeys, final long totalSegmentCacheKeys,
            final long totalBufferedWriteKeys, final long totalDeltaCacheFiles,
            final long compactRequestCount, final long flushRequestCount,
            final long splitScheduleCount, final int splitInFlightCount,
            final int maintenanceQueueSize, final int maintenanceQueueCapacity,
            final int splitQueueSize, final int splitQueueCapacity,
            final long readLatencyP50Micros, final long readLatencyP95Micros,
            final long readLatencyP99Micros, final long writeLatencyP50Micros,
            final long writeLatencyP95Micros, final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
            final boolean walEnabled, final long walAppendCount,
            final long walAppendBytes, final long walSyncCount,
            final long walSyncFailureCount, final long walCorruptionCount,
            final long walTruncationCount, final long walRetainedBytes,
            final int walSegmentCount, final long walDurableLsn,
            final long walCheckpointLsn, final long walPendingSyncBytes,
            final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots,
            final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount,
                registryCacheHitCount, registryCacheMissCount,
                registryCacheLoadCount, registryCacheEvictionCount,
                registryCacheSize, registryCacheLimit,
                segmentCacheKeyLimitPerSegment,
                maxNumberOfKeysInActivePartition,
                maxNumberOfKeysInPartitionBuffer,
                segmentCount, segmentReadyCount, segmentMaintenanceCount,
                segmentErrorCount, segmentClosedCount, segmentBusyCount,
                totalSegmentKeys, totalSegmentCacheKeys, totalBufferedWriteKeys,
                totalDeltaCacheFiles, compactRequestCount, flushRequestCount,
                splitScheduleCount, splitInFlightCount, maintenanceQueueSize,
                maintenanceQueueCapacity, splitQueueSize, splitQueueCapacity,
                readLatencyP50Micros, readLatencyP95Micros,
                readLatencyP99Micros, writeLatencyP50Micros,
                writeLatencyP95Micros, writeLatencyP99Micros,
                bloomFilterHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, bloomFilterRequestCount,
                bloomFilterRefusedCount, bloomFilterPositiveCount,
                bloomFilterFalsePositiveCount, walEnabled, walAppendCount,
                walAppendBytes, walSyncCount, walSyncFailureCount,
                walCorruptionCount, walTruncationCount, walRetainedBytes,
                walSegmentCount, walDurableLsn, walCheckpointLsn,
                walPendingSyncBytes, walDurableLsn, 0L, 0L, 0L, 0L, 0, 0, 0,
                0, 0, 0, 0, 0L, 0L, 0L, 0, segmentRuntimeSnapshots, state);
    }

    /**
     * Full runtime metrics constructor including per-segment and WAL metrics.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int segmentCount, final int segmentReadyCount,
            final int segmentMaintenanceCount, final int segmentErrorCount,
            final int segmentClosedCount, final int segmentBusyCount,
            final long totalSegmentKeys, final long totalSegmentCacheKeys,
            final long totalBufferedWriteKeys, final long totalDeltaCacheFiles,
            final long compactRequestCount, final long flushRequestCount,
            final long splitScheduleCount, final int splitInFlightCount,
            final int maintenanceQueueSize, final int maintenanceQueueCapacity,
            final int splitQueueSize, final int splitQueueCapacity,
            final long readLatencyP50Micros, final long readLatencyP95Micros,
            final long readLatencyP99Micros, final long writeLatencyP50Micros,
            final long writeLatencyP95Micros, final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
            final boolean walEnabled, final long walAppendCount,
            final long walAppendBytes, final long walSyncCount,
            final long walSyncFailureCount, final long walCorruptionCount,
            final long walTruncationCount, final long walRetainedBytes,
            final int walSegmentCount, final long walDurableLsn,
            final long walCheckpointLsn, final long walPendingSyncBytes,
            final long walAppliedLsn,
            final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots,
            final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount,
                registryCacheHitCount, registryCacheMissCount,
                registryCacheLoadCount, registryCacheEvictionCount,
                registryCacheSize, registryCacheLimit,
                segmentCacheKeyLimitPerSegment,
                maxNumberOfKeysInActivePartition,
                maxNumberOfKeysInPartitionBuffer,
                segmentCount, segmentReadyCount, segmentMaintenanceCount,
                segmentErrorCount, segmentClosedCount, segmentBusyCount,
                totalSegmentKeys, totalSegmentCacheKeys, totalBufferedWriteKeys,
                totalDeltaCacheFiles, compactRequestCount, flushRequestCount,
                splitScheduleCount, splitInFlightCount, maintenanceQueueSize,
                maintenanceQueueCapacity, splitQueueSize, splitQueueCapacity,
                readLatencyP50Micros, readLatencyP95Micros,
                readLatencyP99Micros, writeLatencyP50Micros,
                writeLatencyP95Micros, writeLatencyP99Micros,
                bloomFilterHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, bloomFilterRequestCount,
                bloomFilterRefusedCount, bloomFilterPositiveCount,
                bloomFilterFalsePositiveCount, walEnabled, walAppendCount,
                walAppendBytes, walSyncCount, walSyncFailureCount,
                walCorruptionCount, walTruncationCount, walRetainedBytes,
                walSegmentCount, walDurableLsn, walCheckpointLsn,
                walPendingSyncBytes, walAppliedLsn, 0L, 0L, 0L, 0L, 0, 0, 0,
                0, 0, 0, 0, 0L, 0L, 0L, 0, segmentRuntimeSnapshots, state);
    }

    /**
     * Full runtime metrics constructor including per-segment and WAL metrics.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int segmentCount, final int segmentReadyCount,
            final int segmentMaintenanceCount, final int segmentErrorCount,
            final int segmentClosedCount, final int segmentBusyCount,
            final long totalSegmentKeys, final long totalSegmentCacheKeys,
            final long totalBufferedWriteKeys, final long totalDeltaCacheFiles,
            final long compactRequestCount, final long flushRequestCount,
            final long splitScheduleCount, final int splitInFlightCount,
            final int maintenanceQueueSize, final int maintenanceQueueCapacity,
            final int splitQueueSize, final int splitQueueCapacity,
            final long readLatencyP50Micros, final long readLatencyP95Micros,
            final long readLatencyP99Micros, final long writeLatencyP50Micros,
            final long writeLatencyP95Micros, final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
            final boolean walEnabled, final long walAppendCount,
            final long walAppendBytes, final long walSyncCount,
            final long walSyncFailureCount, final long walCorruptionCount,
            final long walTruncationCount, final long walRetainedBytes,
            final int walSegmentCount, final long walDurableLsn,
            final long walCheckpointLsn, final long walPendingSyncBytes,
            final long walAppliedLsn, final long walSyncTotalNanos,
            final long walSyncMaxNanos, final long walSyncBatchBytesTotal,
            final long walSyncBatchBytesMax,
            final int maxNumberOfImmutableRunsPerPartition,
            final int maxNumberOfKeysInIndexBuffer,
            final int partitionCount, final int activePartitionCount,
            final int drainingPartitionCount, final int immutableRunCount,
            final int partitionBufferedKeyCount,
            final long localThrottleCount, final long globalThrottleCount,
            final long drainScheduleCount, final int drainInFlightCount,
            final long drainLatencyP95Micros,
            final List<SegmentMetricsSnapshot> segmentRuntimeSnapshots,
            final SegmentIndexState state) {
        requireNotNegative(getOperationCount, "getOperationCount");
        requireNotNegative(putOperationCount, "putOperationCount");
        requireNotNegative(deleteOperationCount, "deleteOperationCount");
        requireNotNegative(registryCacheHitCount, "registryCacheHitCount");
        requireNotNegative(registryCacheMissCount, "registryCacheMissCount");
        requireNotNegative(registryCacheLoadCount, "registryCacheLoadCount");
        requireNotNegative(registryCacheEvictionCount,
                "registryCacheEvictionCount");
        requireNotNegative(registryCacheSize, "registryCacheSize");
        requireNotNegative(registryCacheLimit, "registryCacheLimit");
        requireNotNegative(segmentCacheKeyLimitPerSegment,
                "segmentCacheKeyLimitPerSegment");
        requireNotNegative(maxNumberOfKeysInActivePartition,
                "maxNumberOfKeysInActivePartition");
        requireNotNegative(maxNumberOfKeysInPartitionBuffer,
                "maxNumberOfKeysInPartitionBuffer");
        requireNotNegative(maxNumberOfImmutableRunsPerPartition,
                "maxNumberOfImmutableRunsPerPartition");
        requireNotNegative(maxNumberOfKeysInIndexBuffer,
                "maxNumberOfKeysInIndexBuffer");
        requireNotNegative(segmentCount, "segmentCount");
        requireNotNegative(segmentReadyCount, "segmentReadyCount");
        requireNotNegative(segmentMaintenanceCount, "segmentMaintenanceCount");
        requireNotNegative(segmentErrorCount, "segmentErrorCount");
        requireNotNegative(segmentClosedCount, "segmentClosedCount");
        requireNotNegative(segmentBusyCount, "segmentBusyCount");
        requireNotNegative(totalSegmentKeys, "totalSegmentKeys");
        requireNotNegative(totalSegmentCacheKeys, "totalSegmentCacheKeys");
        requireNotNegative(totalBufferedWriteKeys, "totalBufferedWriteKeys");
        requireNotNegative(totalDeltaCacheFiles, "totalDeltaCacheFiles");
        requireNotNegative(compactRequestCount, "compactRequestCount");
        requireNotNegative(flushRequestCount, "flushRequestCount");
        requireNotNegative(splitScheduleCount, "splitScheduleCount");
        requireNotNegative(splitInFlightCount, "splitInFlightCount");
        requireNotNegative(maintenanceQueueSize, "maintenanceQueueSize");
        requireNotNegative(maintenanceQueueCapacity,
                "maintenanceQueueCapacity");
        requireNotNegative(splitQueueSize, "splitQueueSize");
        requireNotNegative(splitQueueCapacity, "splitQueueCapacity");
        requireNotNegative(partitionCount, "partitionCount");
        requireNotNegative(activePartitionCount, "activePartitionCount");
        requireNotNegative(drainingPartitionCount,
                "drainingPartitionCount");
        requireNotNegative(immutableRunCount, "immutableRunCount");
        requireNotNegative(partitionBufferedKeyCount,
                "partitionBufferedKeyCount");
        requireNotNegative(localThrottleCount, "localThrottleCount");
        requireNotNegative(globalThrottleCount, "globalThrottleCount");
        requireNotNegative(drainScheduleCount, "drainScheduleCount");
        requireNotNegative(drainInFlightCount, "drainInFlightCount");
        requireNotNegative(drainLatencyP95Micros, "drainLatencyP95Micros");
        requireNotNegative(readLatencyP50Micros, "readLatencyP50Micros");
        requireNotNegative(readLatencyP95Micros, "readLatencyP95Micros");
        requireNotNegative(readLatencyP99Micros, "readLatencyP99Micros");
        requireNotNegative(writeLatencyP50Micros, "writeLatencyP50Micros");
        requireNotNegative(writeLatencyP95Micros, "writeLatencyP95Micros");
        requireNotNegative(writeLatencyP99Micros, "writeLatencyP99Micros");
        requireNotNegative(bloomFilterHashFunctions,
                "bloomFilterHashFunctions");
        requireNotNegative(bloomFilterIndexSizeInBytes,
                "bloomFilterIndexSizeInBytes");
        if (bloomFilterProbabilityOfFalsePositive < 0D) {
            throw new IllegalArgumentException(
                    "bloomFilterProbabilityOfFalsePositive must be >= 0");
        }
        requireNotNegative(bloomFilterRequestCount, "bloomFilterRequestCount");
        requireNotNegative(bloomFilterRefusedCount, "bloomFilterRefusedCount");
        requireNotNegative(bloomFilterPositiveCount,
                "bloomFilterPositiveCount");
        requireNotNegative(bloomFilterFalsePositiveCount,
                "bloomFilterFalsePositiveCount");
        requireNotNegative(walAppendCount, "walAppendCount");
        requireNotNegative(walAppendBytes, "walAppendBytes");
        requireNotNegative(walSyncCount, "walSyncCount");
        requireNotNegative(walSyncFailureCount, "walSyncFailureCount");
        requireNotNegative(walCorruptionCount, "walCorruptionCount");
        requireNotNegative(walTruncationCount, "walTruncationCount");
        requireNotNegative(walRetainedBytes, "walRetainedBytes");
        requireNotNegative(walSegmentCount, "walSegmentCount");
        requireNotNegative(walDurableLsn, "walDurableLsn");
        requireNotNegative(walCheckpointLsn, "walCheckpointLsn");
        requireNotNegative(walPendingSyncBytes, "walPendingSyncBytes");
        requireNotNegative(walAppliedLsn, "walAppliedLsn");
        requireNotNegative(walSyncTotalNanos, "walSyncTotalNanos");
        requireNotNegative(walSyncMaxNanos, "walSyncMaxNanos");
        requireNotNegative(walSyncBatchBytesTotal, "walSyncBatchBytesTotal");
        requireNotNegative(walSyncBatchBytesMax, "walSyncBatchBytesMax");
        this.getOperationCount = getOperationCount;
        this.putOperationCount = putOperationCount;
        this.deleteOperationCount = deleteOperationCount;
        this.registryCacheHitCount = registryCacheHitCount;
        this.registryCacheMissCount = registryCacheMissCount;
        this.registryCacheLoadCount = registryCacheLoadCount;
        this.registryCacheEvictionCount = registryCacheEvictionCount;
        this.registryCacheSize = registryCacheSize;
        this.registryCacheLimit = registryCacheLimit;
        this.segmentCacheKeyLimitPerSegment = segmentCacheKeyLimitPerSegment;
        this.maxNumberOfKeysInActivePartition = maxNumberOfKeysInActivePartition;
        this.maxNumberOfKeysInPartitionBuffer = maxNumberOfKeysInPartitionBuffer;
        this.maxNumberOfImmutableRunsPerPartition = maxNumberOfImmutableRunsPerPartition;
        this.maxNumberOfKeysInIndexBuffer = maxNumberOfKeysInIndexBuffer;
        this.segmentCount = segmentCount;
        this.segmentReadyCount = segmentReadyCount;
        this.segmentMaintenanceCount = segmentMaintenanceCount;
        this.segmentErrorCount = segmentErrorCount;
        this.segmentClosedCount = segmentClosedCount;
        this.segmentBusyCount = segmentBusyCount;
        this.totalSegmentKeys = totalSegmentKeys;
        this.totalSegmentCacheKeys = totalSegmentCacheKeys;
        this.totalBufferedWriteKeys = totalBufferedWriteKeys;
        this.totalDeltaCacheFiles = totalDeltaCacheFiles;
        this.compactRequestCount = compactRequestCount;
        this.flushRequestCount = flushRequestCount;
        this.splitScheduleCount = splitScheduleCount;
        this.splitInFlightCount = splitInFlightCount;
        this.maintenanceQueueSize = maintenanceQueueSize;
        this.maintenanceQueueCapacity = maintenanceQueueCapacity;
        this.splitQueueSize = splitQueueSize;
        this.splitQueueCapacity = splitQueueCapacity;
        this.partitionCount = partitionCount;
        this.activePartitionCount = activePartitionCount;
        this.drainingPartitionCount = drainingPartitionCount;
        this.immutableRunCount = immutableRunCount;
        this.partitionBufferedKeyCount = partitionBufferedKeyCount;
        this.localThrottleCount = localThrottleCount;
        this.globalThrottleCount = globalThrottleCount;
        this.drainScheduleCount = drainScheduleCount;
        this.drainInFlightCount = drainInFlightCount;
        this.drainLatencyP95Micros = drainLatencyP95Micros;
        this.readLatencyP50Micros = readLatencyP50Micros;
        this.readLatencyP95Micros = readLatencyP95Micros;
        this.readLatencyP99Micros = readLatencyP99Micros;
        this.writeLatencyP50Micros = writeLatencyP50Micros;
        this.writeLatencyP95Micros = writeLatencyP95Micros;
        this.writeLatencyP99Micros = writeLatencyP99Micros;
        this.bloomFilterHashFunctions = bloomFilterHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.bloomFilterRequestCount = bloomFilterRequestCount;
        this.bloomFilterRefusedCount = bloomFilterRefusedCount;
        this.bloomFilterPositiveCount = bloomFilterPositiveCount;
        this.bloomFilterFalsePositiveCount = bloomFilterFalsePositiveCount;
        this.walEnabled = walEnabled;
        this.walAppendCount = walAppendCount;
        this.walAppendBytes = walAppendBytes;
        this.walSyncCount = walSyncCount;
        this.walSyncFailureCount = walSyncFailureCount;
        this.walCorruptionCount = walCorruptionCount;
        this.walTruncationCount = walTruncationCount;
        this.walRetainedBytes = walRetainedBytes;
        this.walSegmentCount = walSegmentCount;
        this.walDurableLsn = walDurableLsn;
        this.walCheckpointLsn = walCheckpointLsn;
        this.walPendingSyncBytes = walPendingSyncBytes;
        this.walAppliedLsn = walAppliedLsn;
        this.walSyncTotalNanos = walSyncTotalNanos;
        this.walSyncMaxNanos = walSyncMaxNanos;
        this.walSyncBatchBytesTotal = walSyncBatchBytesTotal;
        this.walSyncBatchBytesMax = walSyncBatchBytesMax;
        final List<SegmentMetricsSnapshot> runtimeMetrics = new ArrayList<>(
                Vldtn.requireNonNull(segmentRuntimeSnapshots,
                        "segmentRuntimeSnapshots"));
        runtimeMetrics.forEach(metric -> Vldtn.requireNonNull(metric,
                "segmentRuntimeSnapshotsItem"));
        this.segmentRuntimeSnapshots = List.copyOf(runtimeMetrics);
        this.state = Vldtn.requireNonNull(state, "state");
    }

    private static void requireNotNegative(final long value,
            final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    public long getGetOperationCount() {
        return getOperationCount;
    }

    public long getPutOperationCount() {
        return putOperationCount;
    }

    public long getDeleteOperationCount() {
        return deleteOperationCount;
    }

    public long getRegistryCacheHitCount() {
        return registryCacheHitCount;
    }

    public long getRegistryCacheMissCount() {
        return registryCacheMissCount;
    }

    public long getRegistryCacheLoadCount() {
        return registryCacheLoadCount;
    }

    public long getRegistryCacheEvictionCount() {
        return registryCacheEvictionCount;
    }

    public int getRegistryCacheSize() {
        return registryCacheSize;
    }

    public int getRegistryCacheLimit() {
        return registryCacheLimit;
    }

    public int getSegmentCacheKeyLimitPerSegment() {
        return segmentCacheKeyLimitPerSegment;
    }

    public int getMaxNumberOfKeysInActivePartition() {
        return maxNumberOfKeysInActivePartition;
    }

    public int getMaxNumberOfImmutableRunsPerPartition() {
        return maxNumberOfImmutableRunsPerPartition;
    }

    public int getMaxNumberOfKeysInPartitionBuffer() {
        return maxNumberOfKeysInPartitionBuffer;
    }

    public int getMaxNumberOfKeysInIndexBuffer() {
        return maxNumberOfKeysInIndexBuffer;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public int getSegmentReadyCount() {
        return segmentReadyCount;
    }

    public int getSegmentMaintenanceCount() {
        return segmentMaintenanceCount;
    }

    public int getSegmentErrorCount() {
        return segmentErrorCount;
    }

    public int getSegmentClosedCount() {
        return segmentClosedCount;
    }

    public int getSegmentBusyCount() {
        return segmentBusyCount;
    }

    public long getTotalSegmentKeys() {
        return totalSegmentKeys;
    }

    public long getTotalSegmentCacheKeys() {
        return totalSegmentCacheKeys;
    }

    public long getTotalBufferedWriteKeys() {
        return totalBufferedWriteKeys;
    }

    public long getTotalDeltaCacheFiles() {
        return totalDeltaCacheFiles;
    }

    public long getCompactRequestCount() {
        return compactRequestCount;
    }

    public long getFlushRequestCount() {
        return flushRequestCount;
    }

    public long getSplitScheduleCount() {
        return splitScheduleCount;
    }

    public int getSplitInFlightCount() {
        return splitInFlightCount;
    }

    public int getMaintenanceQueueSize() {
        return maintenanceQueueSize;
    }

    public int getMaintenanceQueueCapacity() {
        return maintenanceQueueCapacity;
    }

    public int getSplitQueueSize() {
        return splitQueueSize;
    }

    public int getSplitQueueCapacity() {
        return splitQueueCapacity;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getActivePartitionCount() {
        return activePartitionCount;
    }

    public int getDrainingPartitionCount() {
        return drainingPartitionCount;
    }

    public int getImmutableRunCount() {
        return immutableRunCount;
    }

    public int getPartitionBufferedKeyCount() {
        return partitionBufferedKeyCount;
    }

    public long getLocalThrottleCount() {
        return localThrottleCount;
    }

    public long getGlobalThrottleCount() {
        return globalThrottleCount;
    }

    public long getDrainScheduleCount() {
        return drainScheduleCount;
    }

    public int getDrainInFlightCount() {
        return drainInFlightCount;
    }

    public long getDrainLatencyP95Micros() {
        return drainLatencyP95Micros;
    }

    public long getReadLatencyP50Micros() {
        return readLatencyP50Micros;
    }

    public long getReadLatencyP95Micros() {
        return readLatencyP95Micros;
    }

    public long getReadLatencyP99Micros() {
        return readLatencyP99Micros;
    }

    public long getWriteLatencyP50Micros() {
        return writeLatencyP50Micros;
    }

    public long getWriteLatencyP95Micros() {
        return writeLatencyP95Micros;
    }

    public long getWriteLatencyP99Micros() {
        return writeLatencyP99Micros;
    }

    public int getBloomFilterHashFunctions() {
        return bloomFilterHashFunctions;
    }

    public int getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    public double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    public long getBloomFilterRequestCount() {
        return bloomFilterRequestCount;
    }

    public long getBloomFilterRefusedCount() {
        return bloomFilterRefusedCount;
    }

    public long getBloomFilterPositiveCount() {
        return bloomFilterPositiveCount;
    }

    public long getBloomFilterFalsePositiveCount() {
        return bloomFilterFalsePositiveCount;
    }

    public boolean isWalEnabled() {
        return walEnabled;
    }

    public long getWalAppendCount() {
        return walAppendCount;
    }

    public long getWalAppendBytes() {
        return walAppendBytes;
    }

    public long getWalSyncCount() {
        return walSyncCount;
    }

    public long getWalSyncFailureCount() {
        return walSyncFailureCount;
    }

    public long getWalCorruptionCount() {
        return walCorruptionCount;
    }

    public long getWalTruncationCount() {
        return walTruncationCount;
    }

    public long getWalRetainedBytes() {
        return walRetainedBytes;
    }

    public int getWalSegmentCount() {
        return walSegmentCount;
    }

    public long getWalDurableLsn() {
        return walDurableLsn;
    }

    public long getWalCheckpointLsn() {
        return walCheckpointLsn;
    }

    public long getWalPendingSyncBytes() {
        return walPendingSyncBytes;
    }

    public long getWalAppliedLsn() {
        return walAppliedLsn;
    }

    public long getWalCheckpointLagLsn() {
        return Math.max(0L, walAppliedLsn - walCheckpointLsn);
    }

    public long getWalSyncTotalNanos() {
        return walSyncTotalNanos;
    }

    public long getWalSyncMaxNanos() {
        return walSyncMaxNanos;
    }

    public long getWalSyncBatchBytesTotal() {
        return walSyncBatchBytesTotal;
    }

    public long getWalSyncBatchBytesMax() {
        return walSyncBatchBytesMax;
    }

    public long getWalSyncAvgNanos() {
        if (walSyncCount <= 0L) {
            return 0L;
        }
        return walSyncTotalNanos / walSyncCount;
    }

    public long getWalSyncAvgBatchBytes() {
        if (walSyncCount <= 0L) {
            return 0L;
        }
        return walSyncBatchBytesTotal / walSyncCount;
    }

    public List<SegmentMetricsSnapshot> getSegmentRuntimeSnapshots() {
        return segmentRuntimeSnapshots;
    }

    public SegmentIndexState getState() {
        return state;
    }

    /**
     * Immutable per-segment metrics captured as part of index runtime snapshot.
     */
    public static final class SegmentMetricsSnapshot {
        private final String segmentId;
        private final SegmentState state;
        private final long numberOfKeysInDeltaCache;
        private final long numberOfKeysInSegment;
        private final long numberOfKeysInScarceIndex;
        private final long numberOfKeysInSegmentCache;
        private final int numberOfKeysInWriteCache;
        private final int numberOfDeltaCacheFiles;
        private final long compactRequestCount;
        private final long flushRequestCount;
        private final long bloomFilterRequestCount;
        private final long bloomFilterRefusedCount;
        private final long bloomFilterPositiveCount;
        private final long bloomFilterFalsePositiveCount;

        /**
         * Creates per-segment metrics from one runtime snapshot.
         *
         * @param runtimeSnapshot segment runtime snapshot
         */
        public SegmentMetricsSnapshot(
                final SegmentRuntimeSnapshot runtimeSnapshot) {
            this(Vldtn.requireNonNull(runtimeSnapshot, "runtimeSnapshot")
                    .getSegmentId().getName(),
                    runtimeSnapshot.getState(),
                    runtimeSnapshot.getNumberOfKeysInDeltaCache(),
                    runtimeSnapshot.getNumberOfKeysInSegment(),
                    runtimeSnapshot.getNumberOfKeysInScarceIndex(),
                    runtimeSnapshot.getNumberOfKeysInSegmentCache(),
                    runtimeSnapshot.getNumberOfKeysInWriteCache(),
                    runtimeSnapshot.getNumberOfDeltaCacheFiles(),
                    runtimeSnapshot.getNumberOfCompacts(),
                    runtimeSnapshot.getNumberOfFlushes(),
                    runtimeSnapshot.getBloomFilterRequestCount(),
                    runtimeSnapshot.getBloomFilterRefusedCount(),
                    runtimeSnapshot.getBloomFilterPositiveCount(),
                    runtimeSnapshot.getBloomFilterFalsePositiveCount());
        }

        public SegmentMetricsSnapshot(final String segmentId,
                final SegmentState state, final long numberOfKeysInDeltaCache,
                final long numberOfKeysInSegment,
                final long numberOfKeysInScarceIndex,
                final long numberOfKeysInSegmentCache,
                final int numberOfKeysInWriteCache,
                final int numberOfDeltaCacheFiles,
                final long compactRequestCount, final long flushRequestCount,
                final long bloomFilterRequestCount,
                final long bloomFilterRefusedCount,
                final long bloomFilterPositiveCount,
                final long bloomFilterFalsePositiveCount) {
            this.segmentId = Vldtn.requireNotBlank(segmentId, "segmentId");
            this.state = Vldtn.requireNonNull(state, "state");
            requireNotNegative(numberOfKeysInDeltaCache,
                    "numberOfKeysInDeltaCache");
            requireNotNegative(numberOfKeysInSegment, "numberOfKeysInSegment");
            requireNotNegative(numberOfKeysInScarceIndex,
                    "numberOfKeysInScarceIndex");
            requireNotNegative(numberOfKeysInSegmentCache,
                    "numberOfKeysInSegmentCache");
            requireNotNegative(numberOfKeysInWriteCache,
                    "numberOfKeysInWriteCache");
            requireNotNegative(numberOfDeltaCacheFiles,
                    "numberOfDeltaCacheFiles");
            requireNotNegative(compactRequestCount, "compactRequestCount");
            requireNotNegative(flushRequestCount, "flushRequestCount");
            requireNotNegative(bloomFilterRequestCount,
                    "bloomFilterRequestCount");
            requireNotNegative(bloomFilterRefusedCount,
                    "bloomFilterRefusedCount");
            requireNotNegative(bloomFilterPositiveCount,
                    "bloomFilterPositiveCount");
            requireNotNegative(bloomFilterFalsePositiveCount,
                    "bloomFilterFalsePositiveCount");
            this.numberOfKeysInDeltaCache = numberOfKeysInDeltaCache;
            this.numberOfKeysInSegment = numberOfKeysInSegment;
            this.numberOfKeysInScarceIndex = numberOfKeysInScarceIndex;
            this.numberOfKeysInSegmentCache = numberOfKeysInSegmentCache;
            this.numberOfKeysInWriteCache = numberOfKeysInWriteCache;
            this.numberOfDeltaCacheFiles = numberOfDeltaCacheFiles;
            this.compactRequestCount = compactRequestCount;
            this.flushRequestCount = flushRequestCount;
            this.bloomFilterRequestCount = bloomFilterRequestCount;
            this.bloomFilterRefusedCount = bloomFilterRefusedCount;
            this.bloomFilterPositiveCount = bloomFilterPositiveCount;
            this.bloomFilterFalsePositiveCount = bloomFilterFalsePositiveCount;
        }

        public String getSegmentId() {
            return segmentId;
        }

        public SegmentState getState() {
            return state;
        }

        public long getNumberOfKeysInDeltaCache() {
            return numberOfKeysInDeltaCache;
        }

        public long getNumberOfKeysInSegment() {
            return numberOfKeysInSegment;
        }

        public long getNumberOfKeysInScarceIndex() {
            return numberOfKeysInScarceIndex;
        }

        public long getNumberOfKeysInSegmentCache() {
            return numberOfKeysInSegmentCache;
        }

        public int getNumberOfKeysInWriteCache() {
            return numberOfKeysInWriteCache;
        }

        public int getNumberOfDeltaCacheFiles() {
            return numberOfDeltaCacheFiles;
        }

        public long getCompactRequestCount() {
            return compactRequestCount;
        }

        public long getFlushRequestCount() {
            return flushRequestCount;
        }

        public long getBloomFilterRequestCount() {
            return bloomFilterRequestCount;
        }

        public long getBloomFilterRefusedCount() {
            return bloomFilterRefusedCount;
        }

        public long getBloomFilterPositiveCount() {
            return bloomFilterPositiveCount;
        }

        public long getBloomFilterFalsePositiveCount() {
            return bloomFilterFalsePositiveCount;
        }
    }
}
