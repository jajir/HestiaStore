package org.hestiastore.index.segmentindex.metrics;

import java.util.List;

import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;

/**
 * Mutable aggregate of loaded stable-segment runtime metrics.
 */
final class StableSegmentRuntimeMetrics {

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
    private final java.util.List<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot> stableSegmentMetricsSnapshots =
            new java.util.ArrayList<>();

    void setTotalMappedStableSegmentCount(final int count) {
        totalMappedStableSegmentCount = Math.max(0, count);
    }

    int getTotalMappedStableSegmentCount() {
        return totalMappedStableSegmentCount;
    }

    void incrementReadyStableSegmentCount() {
        readyStableSegmentCount++;
    }

    int getReadyStableSegmentCount() {
        return readyStableSegmentCount;
    }

    void incrementStableSegmentsInMaintenanceStateCount() {
        stableSegmentsInMaintenanceStateCount++;
    }

    int getStableSegmentsInMaintenanceStateCount() {
        return stableSegmentsInMaintenanceStateCount;
    }

    void incrementErrorStableSegmentCount() {
        errorStableSegmentCount++;
    }

    int getErrorStableSegmentCount() {
        return errorStableSegmentCount;
    }

    void incrementClosedStableSegmentCount() {
        closedStableSegmentCount++;
    }

    int getClosedStableSegmentCount() {
        return closedStableSegmentCount;
    }

    void setUnloadedMappedStableSegmentCount(final int count) {
        unloadedMappedStableSegmentCount = Math.max(0, count);
    }

    int getUnloadedMappedStableSegmentCount() {
        return unloadedMappedStableSegmentCount;
    }

    void addSegmentRuntimeSnapshot(final SegmentRuntimeSnapshot segmentRuntime) {
        totalStableSegmentKeyCount += Math.max(0L,
                segmentRuntime.getNumberOfKeys());
        totalStableSegmentCacheKeyCount += Math.max(0L,
                segmentRuntime.getNumberOfKeysInSegmentCache());
        totalStableSegmentWriteBufferKeyCount += Math.max(0L,
                segmentRuntime.getNumberOfKeysInWriteCache());
        totalStableSegmentDeltaCacheFileCount += Math.max(0,
                segmentRuntime.getNumberOfDeltaCacheFiles());
        totalCompactRequestCount += Math.max(0L,
                segmentRuntime.getNumberOfCompacts());
        totalFlushRequestCount += Math.max(0L,
                segmentRuntime.getNumberOfFlushes());
        totalBloomFilterRequestCount += Math.max(0L,
                segmentRuntime.getBloomFilterRequestCount());
        totalBloomFilterRefusedCount += Math.max(0L,
                segmentRuntime.getBloomFilterRefusedCount());
        totalBloomFilterPositiveCount += Math.max(0L,
                segmentRuntime.getBloomFilterPositiveCount());
        totalBloomFilterFalsePositiveCount += Math.max(0L,
                segmentRuntime.getBloomFilterFalsePositiveCount());
        stableSegmentMetricsSnapshots
                .add(new SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot(
                        segmentRuntime));
    }

    long getTotalStableSegmentKeyCount() {
        return totalStableSegmentKeyCount;
    }

    long getTotalStableSegmentCacheKeyCount() {
        return totalStableSegmentCacheKeyCount;
    }

    long getTotalStableSegmentWriteBufferKeyCount() {
        return totalStableSegmentWriteBufferKeyCount;
    }

    long getTotalStableSegmentDeltaCacheFileCount() {
        return totalStableSegmentDeltaCacheFileCount;
    }

    long getTotalCompactRequestCount() {
        return totalCompactRequestCount;
    }

    long getTotalFlushRequestCount() {
        return totalFlushRequestCount;
    }

    long getTotalBloomFilterRequestCount() {
        return totalBloomFilterRequestCount;
    }

    long getTotalBloomFilterRefusedCount() {
        return totalBloomFilterRefusedCount;
    }

    long getTotalBloomFilterPositiveCount() {
        return totalBloomFilterPositiveCount;
    }

    long getTotalBloomFilterFalsePositiveCount() {
        return totalBloomFilterFalsePositiveCount;
    }

    List<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot> getStableSegmentMetricsSnapshots() {
        return stableSegmentMetricsSnapshots;
    }
}
