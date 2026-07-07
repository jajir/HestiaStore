package org.hestiastore.index.segmentindex.monitoring;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;

/**
 * Mutable aggregate of loaded stable-segment runtime metrics.
 */
final class SegmentRuntimeMetrics {

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
    private final List<SegmentRuntimeSnapshot> stableSegmentRuntimeSnapshots =
            new ArrayList<>();

    void setTotalMappedStableSegmentCount(final int count) {
        totalMappedStableSegmentCount = nonNegativeInt(count,
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
        unloadedMappedStableSegmentCount = nonNegativeInt(count,
                "unloadedMappedStableSegmentCount");
    }

    int getUnloadedMappedStableSegmentCount() {
        return unloadedMappedStableSegmentCount;
    }

    void addSegmentRuntimeSnapshot(final SegmentRuntimeSnapshot segmentRuntime) {
        final SegmentRuntimeSnapshot snapshot = Vldtn.requireNonNull(
                segmentRuntime, "segmentRuntime");
        final long numberOfKeys = nonNegativeLong(snapshot.getNumberOfKeys(),
                "numberOfKeys");
        final long numberOfKeysInSegmentCache = nonNegativeLong(
                snapshot.getNumberOfKeysInSegmentCache(),
                "numberOfKeysInSegmentCache");
        final int numberOfKeysInWriteCache = nonNegativeInt(
                snapshot.getNumberOfKeysInWriteCache(),
                "numberOfKeysInWriteCache");
        final int numberOfDeltaCacheFiles = nonNegativeInt(
                snapshot.getNumberOfDeltaCacheFiles(),
                "numberOfDeltaCacheFiles");
        final long compactRequestCount = nonNegativeLong(
                snapshot.getNumberOfCompacts(), "compactRequestCount");
        final long flushRequestCount = nonNegativeLong(
                snapshot.getNumberOfFlushes(), "flushRequestCount");
        final long bloomFilterRequestCount = nonNegativeLong(
                snapshot.getBloomFilterRequestCount(),
                "bloomFilterRequestCount");
        final long bloomFilterRefusedCount = nonNegativeLong(
                snapshot.getBloomFilterRefusedCount(),
                "bloomFilterRefusedCount");
        final long bloomFilterPositiveCount = nonNegativeLong(
                snapshot.getBloomFilterPositiveCount(),
                "bloomFilterPositiveCount");
        final long bloomFilterFalsePositiveCount = nonNegativeLong(
                snapshot.getBloomFilterFalsePositiveCount(),
                "bloomFilterFalsePositiveCount");

        totalStableSegmentKeyCount += numberOfKeys;
        totalStableSegmentCacheKeyCount += numberOfKeysInSegmentCache;
        totalStableSegmentWriteBufferKeyCount +=
                numberOfKeysInWriteCache;
        totalStableSegmentDeltaCacheFileCount +=
                numberOfDeltaCacheFiles;
        totalCompactRequestCount += compactRequestCount;
        totalFlushRequestCount += flushRequestCount;
        totalBloomFilterRequestCount += bloomFilterRequestCount;
        totalBloomFilterRefusedCount += bloomFilterRefusedCount;
        totalBloomFilterPositiveCount += bloomFilterPositiveCount;
        totalBloomFilterFalsePositiveCount +=
                bloomFilterFalsePositiveCount;
        stableSegmentRuntimeSnapshots.add(snapshot);
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

    List<SegmentRuntimeSnapshot> getStableSegmentRuntimeSnapshots() {
        return List.copyOf(stableSegmentRuntimeSnapshots);
    }

    private static int nonNegativeInt(final int value, final String name) {
        return Vldtn.requireGreaterThanOrEqualToZero(value, name);
    }

    private static long nonNegativeLong(final long value, final String name) {
        return Vldtn.requireGreaterThanOrEqualToZero(value, name);
    }
}
