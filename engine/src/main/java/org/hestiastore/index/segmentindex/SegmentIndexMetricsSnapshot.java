package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Immutable snapshot of index and segment runtime metrics.
 */
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
    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    private final int segmentCount;
    private final int segmentReadyCount;
    private final int segmentMaintenanceCount;
    private final int segmentErrorCount;
    private final int segmentClosedCount;
    private final int segmentBusyCount;
    private final long totalSegmentKeys;
    private final long totalSegmentCacheKeys;
    private final long totalWriteCacheKeys;
    private final long totalDeltaCacheFiles;
    private final long compactRequestCount;
    private final long flushRequestCount;
    private final long splitScheduleCount;
    private final int splitInFlightCount;
    private final int maintenanceQueueSize;
    private final int maintenanceQueueCapacity;
    private final int splitQueueSize;
    private final int splitQueueCapacity;
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
    private final SegmentIndexState state;

    /**
     * Backward-compatible constructor with only operation counters.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount, 0L,
                0L, 0L, 0L, 0, 0, state);
    }

    /**
     * Backward-compatible constructor with registry cache counters.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount,
            final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit, final SegmentIndexState state) {
        this(getOperationCount, putOperationCount, deleteOperationCount,
                registryCacheHitCount, registryCacheMissCount,
                registryCacheLoadCount, registryCacheEvictionCount,
                registryCacheSize, registryCacheLimit, 0, 0, 0, 0, 0, 0, 0, 0,
                0,
                0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0,
                0D, 0L, 0L, 0L, 0L, state);
    }

    /**
     * Full runtime metrics constructor.
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final long registryCacheHitCount,
            final long registryCacheMissCount,
            final long registryCacheLoadCount,
            final long registryCacheEvictionCount, final int registryCacheSize,
            final int registryCacheLimit,
            final int segmentCacheKeyLimitPerSegment,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
            final int segmentCount,
            final int segmentReadyCount, final int segmentMaintenanceCount,
            final int segmentErrorCount, final int segmentClosedCount,
            final int segmentBusyCount, final long totalSegmentKeys,
            final long totalSegmentCacheKeys, final long totalWriteCacheKeys,
            final long totalDeltaCacheFiles, final long compactRequestCount,
            final long flushRequestCount, final long splitScheduleCount,
            final int splitInFlightCount, final int maintenanceQueueSize,
            final int maintenanceQueueCapacity, final int splitQueueSize,
            final int splitQueueCapacity, final long readLatencyP50Micros,
            final long readLatencyP95Micros, final long readLatencyP99Micros,
            final long writeLatencyP50Micros,
            final long writeLatencyP95Micros,
            final long writeLatencyP99Micros,
            final int bloomFilterHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount,
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
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
        this.segmentCount = segmentCount;
        this.segmentReadyCount = segmentReadyCount;
        this.segmentMaintenanceCount = segmentMaintenanceCount;
        this.segmentErrorCount = segmentErrorCount;
        this.segmentClosedCount = segmentClosedCount;
        this.segmentBusyCount = segmentBusyCount;
        this.totalSegmentKeys = totalSegmentKeys;
        this.totalSegmentCacheKeys = totalSegmentCacheKeys;
        this.totalWriteCacheKeys = totalWriteCacheKeys;
        this.totalDeltaCacheFiles = totalDeltaCacheFiles;
        this.compactRequestCount = compactRequestCount;
        this.flushRequestCount = flushRequestCount;
        this.splitScheduleCount = splitScheduleCount;
        this.splitInFlightCount = splitInFlightCount;
        this.maintenanceQueueSize = maintenanceQueueSize;
        this.maintenanceQueueCapacity = maintenanceQueueCapacity;
        this.splitQueueSize = splitQueueSize;
        this.splitQueueCapacity = splitQueueCapacity;
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

    public int getMaxNumberOfKeysInSegmentWriteCache() {
        return maxNumberOfKeysInSegmentWriteCache;
    }

    public int getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance() {
        return maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
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

    public long getTotalWriteCacheKeys() {
        return totalWriteCacheKeys;
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

    public SegmentIndexState getState() {
        return state;
    }
}
