package org.hestiastore.index.segmentindex.runtimemonitoring;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexSegmentRuntimeMetrics;

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
    private final List<SegmentIndexSegmentRuntimeMetrics> stableSegmentMetricsSnapshots =
            new ArrayList<>();

    void setTotalMappedStableSegmentCount(final int count) {
        totalMappedStableSegmentCount = nonNegative(count,
                "totalMappedStableSegmentCount");
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
        unloadedMappedStableSegmentCount = nonNegative(count,
                "unloadedMappedStableSegmentCount");
    }

    int getUnloadedMappedStableSegmentCount() {
        return unloadedMappedStableSegmentCount;
    }

    void addSegmentRuntimeSnapshot(final SegmentRuntimeSnapshot segmentRuntime) {
        final SegmentIndexSegmentRuntimeMetrics metrics =
                new SegmentIndexSegmentRuntimeMetrics(
                        segmentRuntime.getSegmentId().getName(),
                        segmentRuntime.getState(),
                        segmentRuntime.getNumberOfKeysInDeltaCache(),
                        segmentRuntime.getNumberOfKeysInSegment(),
                        segmentRuntime.getNumberOfKeysInScarceIndex(),
                        segmentRuntime.getNumberOfKeysInSegmentCache(),
                        segmentRuntime.getNumberOfKeysInWriteCache(),
                        segmentRuntime.getNumberOfDeltaCacheFiles(),
                        segmentRuntime.getNumberOfCompacts(),
                        segmentRuntime.getNumberOfFlushes(),
                        segmentRuntime.getBloomFilterRequestCount(),
                        segmentRuntime.getBloomFilterRefusedCount(),
                        segmentRuntime.getBloomFilterPositiveCount(),
                        segmentRuntime.getBloomFilterFalsePositiveCount());
        totalStableSegmentKeyCount += nonNegative(segmentRuntime.getNumberOfKeys(),
                "numberOfKeys");
        totalStableSegmentCacheKeyCount += metrics.numberOfKeysInSegmentCache();
        totalStableSegmentWriteBufferKeyCount +=
                metrics.numberOfKeysInWriteCache();
        totalStableSegmentDeltaCacheFileCount +=
                metrics.numberOfDeltaCacheFiles();
        totalCompactRequestCount += metrics.compactRequestCount();
        totalFlushRequestCount += metrics.flushRequestCount();
        totalBloomFilterRequestCount += metrics.bloomFilterRequestCount();
        totalBloomFilterRefusedCount += metrics.bloomFilterRefusedCount();
        totalBloomFilterPositiveCount += metrics.bloomFilterPositiveCount();
        totalBloomFilterFalsePositiveCount +=
                metrics.bloomFilterFalsePositiveCount();
        stableSegmentMetricsSnapshots.add(metrics);
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

    List<SegmentIndexSegmentRuntimeMetrics> getStableSegmentMetricsSnapshots() {
        return stableSegmentMetricsSnapshots;
    }

    private static int nonNegative(final int value, final String name) {
        return Vldtn.requireGreaterThanOrEqualToZero(value, name);
    }

    private static long nonNegative(final long value, final String name) {
        return Vldtn.requireGreaterThanOrEqualToZero(value, name);
    }
}
