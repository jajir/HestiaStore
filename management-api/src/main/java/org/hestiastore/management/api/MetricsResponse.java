package org.hestiastore.management.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Metrics payload returned by management API.
 */
public record MetricsResponse(String indexName, String state,
        long getOperationCount, long putOperationCount,
        long deleteOperationCount, long registryCacheHitCount,
        long registryCacheMissCount, long registryCacheLoadCount,
        long registryCacheEvictionCount, int registryCacheSize,
        int registryCacheLimit, int segmentCacheKeyLimitPerSegment,
        int maxNumberOfKeysInSegmentWriteCache,
        int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
        int segmentCount, int segmentReadyCount,
        int segmentMaintenanceCount, int segmentErrorCount,
        int segmentClosedCount, int segmentBusyCount, long totalSegmentKeys,
        long totalSegmentCacheKeys, long totalWriteCacheKeys,
        long totalDeltaCacheFiles, long compactRequestCount,
        long flushRequestCount, long splitScheduleCount, int splitInFlightCount,
        int maintenanceQueueSize, int maintenanceQueueCapacity,
        int splitQueueSize, int splitQueueCapacity, long readLatencyP50Micros,
        long readLatencyP95Micros, long readLatencyP99Micros,
        long writeLatencyP50Micros, long writeLatencyP95Micros,
        long writeLatencyP99Micros, int bloomFilterHashFunctions,
        int bloomFilterIndexSizeInBytes,
        double bloomFilterProbabilityOfFalsePositive,
        long bloomFilterRequestCount, long bloomFilterRefusedCount,
        long bloomFilterPositiveCount, long bloomFilterFalsePositiveCount,
        long jvmHeapUsedBytes, long jvmHeapCommittedBytes,
        long jvmNonHeapUsedBytes, long jvmGcCount, long jvmGcTimeMillis,
        Instant capturedAt) {

    /**
     * Validating canonical constructor.
     */
    public MetricsResponse {
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
        requireNotNegative(maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        requireNotNegative(maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
        requireNotNegative(segmentCount, "segmentCount");
        requireNotNegative(segmentReadyCount, "segmentReadyCount");
        requireNotNegative(segmentMaintenanceCount, "segmentMaintenanceCount");
        requireNotNegative(segmentErrorCount, "segmentErrorCount");
        requireNotNegative(segmentClosedCount, "segmentClosedCount");
        requireNotNegative(segmentBusyCount, "segmentBusyCount");
        requireNotNegative(totalSegmentKeys, "totalSegmentKeys");
        requireNotNegative(totalSegmentCacheKeys, "totalSegmentCacheKeys");
        requireNotNegative(totalWriteCacheKeys, "totalWriteCacheKeys");
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
        requireNotNegative(jvmHeapUsedBytes, "jvmHeapUsedBytes");
        requireNotNegative(jvmHeapCommittedBytes, "jvmHeapCommittedBytes");
        requireNotNegative(jvmNonHeapUsedBytes, "jvmNonHeapUsedBytes");
        requireNotNegative(jvmGcCount, "jvmGcCount");
        requireNotNegative(jvmGcTimeMillis, "jvmGcTimeMillis");
        capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
    }

    /**
     * Backward-compatible constructor.
     */
    public MetricsResponse(final String indexName, final String state,
            final long getOperationCount, final long putOperationCount,
            final long deleteOperationCount, final Instant capturedAt) {
        this(indexName, state, getOperationCount, putOperationCount,
                deleteOperationCount,
                0L, 0L, 0L, 0L,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L,
                0, 0, 0D, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L,
                capturedAt);
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
