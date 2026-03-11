package org.hestiastore.monitoring.json.api;

import java.util.List;
import java.util.Objects;

/**
 * Per-index metrics section inside node report payload.
 */
public record IndexReportResponse(String indexName, String state,
        boolean ready,
        long getOperationCount, long putOperationCount,
        long deleteOperationCount, long registryCacheHitCount,
        long registryCacheMissCount, long registryCacheLoadCount,
        long registryCacheEvictionCount, int registryCacheSize,
        int registryCacheLimit, int segmentCacheKeyLimitPerSegment,
        int maxNumberOfKeysInActivePartition,
        int maxNumberOfImmutableRunsPerPartition,
        int maxNumberOfKeysInPartitionBuffer,
        int maxNumberOfKeysInIndexBuffer,
        int segmentCount, int segmentReadyCount,
        int segmentMaintenanceCount, int segmentErrorCount,
        int segmentClosedCount, int segmentBusyCount, long totalSegmentKeys,
        long totalSegmentCacheKeys, long totalBufferedWriteKeys,
        long totalDeltaCacheFiles, long compactRequestCount,
        long flushRequestCount, long splitScheduleCount, int splitInFlightCount,
        int maintenanceQueueSize, int maintenanceQueueCapacity,
        int splitQueueSize, int splitQueueCapacity, int partitionCount,
        int activePartitionCount, int drainingPartitionCount,
        int immutableRunCount, int partitionBufferedKeyCount,
        long localThrottleCount, long globalThrottleCount,
        long drainScheduleCount, int drainInFlightCount,
        long readLatencyP50Micros, long readLatencyP95Micros,
        long readLatencyP99Micros, long writeLatencyP50Micros,
        long writeLatencyP95Micros, long writeLatencyP99Micros,
        int bloomFilterHashFunctions,
        int bloomFilterIndexSizeInBytes,
        double bloomFilterProbabilityOfFalsePositive,
        long bloomFilterRequestCount, long bloomFilterRefusedCount,
        long bloomFilterPositiveCount, long bloomFilterFalsePositiveCount,
        List<SegmentRuntimeReportResponse> segmentRuntimeSnapshots) {

    /**
     * Creates validated per-index metrics payload.
     */
    public IndexReportResponse {
        indexName = normalize(indexName, "indexName");
        state = normalize(state, "state");
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
        requireNotNegative(maxNumberOfImmutableRunsPerPartition,
                "maxNumberOfImmutableRunsPerPartition");
        requireNotNegative(maxNumberOfKeysInPartitionBuffer,
                "maxNumberOfKeysInPartitionBuffer");
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
        segmentRuntimeSnapshots = List.copyOf(
                Objects.requireNonNull(segmentRuntimeSnapshots,
                        "segmentRuntimeSnapshots"));
    }

    /**
     * Backward-compatible constructor without per-segment section.
     */
    public IndexReportResponse(final String indexName, final String state,
            final boolean ready, final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount, final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfImmutableRunsPerPartition,
            final int maxNumberOfKeysInPartitionBuffer,
            final int maxNumberOfKeysInIndexBuffer,
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
            final long bloomFilterFalsePositiveCount) {
        this(indexName, state, ready, getOperationCount, putOperationCount,
                deleteOperationCount, registryCacheHitCount,
                registryCacheMissCount, registryCacheLoadCount,
                registryCacheEvictionCount, registryCacheSize,
                registryCacheLimit, segmentCacheKeyLimitPerSegment,
                maxNumberOfKeysInActivePartition,
                maxNumberOfImmutableRunsPerPartition,
                maxNumberOfKeysInPartitionBuffer,
                maxNumberOfKeysInIndexBuffer,
                segmentCount, segmentReadyCount, segmentMaintenanceCount,
                segmentErrorCount, segmentClosedCount, segmentBusyCount,
                totalSegmentKeys, totalSegmentCacheKeys, totalBufferedWriteKeys,
                totalDeltaCacheFiles, compactRequestCount, flushRequestCount,
                splitScheduleCount, splitInFlightCount, maintenanceQueueSize,
                maintenanceQueueCapacity, splitQueueSize, splitQueueCapacity,
                0, 0, 0, 0, 0, 0L, 0L, 0L, 0,
                readLatencyP50Micros, readLatencyP95Micros, readLatencyP99Micros,
                writeLatencyP50Micros, writeLatencyP95Micros,
                writeLatencyP99Micros, bloomFilterHashFunctions,
                bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, bloomFilterRequestCount,
                bloomFilterRefusedCount, bloomFilterPositiveCount,
                bloomFilterFalsePositiveCount, List.of());
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }

    private static void requireNotNegative(final long value,
            final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
