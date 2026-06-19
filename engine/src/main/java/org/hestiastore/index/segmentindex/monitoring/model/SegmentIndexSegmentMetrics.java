package org.hestiastore.index.segmentindex.monitoring.model;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * User-facing aggregate segment metrics.
 */
public final class SegmentIndexSegmentMetrics {

    private final int cacheKeyLimitPerSegment;
    private final int count;
    private final int readyCount;
    private final int maintenanceCount;
    private final int errorCount;
    private final int closedCount;
    private final int unloadedMappedSegmentCount;
    private final long totalKeys;
    private final long totalCacheKeys;
    private final long totalDeltaCacheFiles;
    private final List<SegmentIndexSegmentRuntimeMetrics> runtimeMetrics;

    /**
     * Creates aggregate segment metrics.
     *
     * @param cacheKeyLimitPerSegment configured cache key limit per segment
     * @param count mapped segment count
     * @param readyCount ready segment count
     * @param maintenanceCount maintenance segment count
     * @param errorCount error segment count
     * @param closedCount closed segment count
     * @param unloadedMappedSegmentCount mapped but not currently loaded segment
     *            count
     * @param totalKeys total key count
     * @param totalCacheKeys total cache key count
     * @param totalDeltaCacheFiles total delta cache file count
     * @param runtimeMetrics per-segment runtime metrics
     */
    @SuppressWarnings("java:S107")
    public SegmentIndexSegmentMetrics(final int cacheKeyLimitPerSegment,
            final int count, final int readyCount, final int maintenanceCount,
            final int errorCount, final int closedCount,
            final int unloadedMappedSegmentCount,
            final long totalKeys, final long totalCacheKeys,
            final long totalDeltaCacheFiles,
            final List<SegmentIndexSegmentRuntimeMetrics> runtimeMetrics) {
        this.cacheKeyLimitPerSegment = MetricModelValidation.nonNegative(
                cacheKeyLimitPerSegment, "cacheKeyLimitPerSegment");
        this.count = MetricModelValidation.nonNegative(count, "count");
        this.readyCount = MetricModelValidation.nonNegative(readyCount,
                "readyCount");
        this.maintenanceCount = MetricModelValidation.nonNegative(
                maintenanceCount, "maintenanceCount");
        this.errorCount = MetricModelValidation.nonNegative(errorCount,
                "errorCount");
        this.closedCount = MetricModelValidation.nonNegative(closedCount,
                "closedCount");
        this.unloadedMappedSegmentCount = MetricModelValidation.nonNegative(
                unloadedMappedSegmentCount, "unloadedMappedSegmentCount");
        this.totalKeys = MetricModelValidation.nonNegative(totalKeys,
                "totalKeys");
        this.totalCacheKeys = MetricModelValidation.nonNegative(totalCacheKeys,
                "totalCacheKeys");
        this.totalDeltaCacheFiles = MetricModelValidation.nonNegative(
                totalDeltaCacheFiles, "totalDeltaCacheFiles");
        final List<SegmentIndexSegmentRuntimeMetrics> values =
                new ArrayList<>(Vldtn.requireNonNull(runtimeMetrics,
                        "runtimeMetrics"));
        values.forEach(value -> Vldtn.requireNonNull(value,
                "runtimeMetricsItem"));
        this.runtimeMetrics = List.copyOf(values);
    }

    /**
     * Returns configured cache key limit per segment.
     *
     * @return cache key limit per segment
     */
    public int cacheKeyLimitPerSegment() {
        return cacheKeyLimitPerSegment;
    }

    /**
     * Returns mapped segment count.
     *
     * @return segment count
     */
    public int count() {
        return count;
    }

    /**
     * Returns ready segment count.
     *
     * @return ready segment count
     */
    public int readyCount() {
        return readyCount;
    }

    /**
     * Returns maintenance segment count.
     *
     * @return maintenance segment count
     */
    public int maintenanceCount() {
        return maintenanceCount;
    }

    /**
     * Returns error segment count.
     *
     * @return error segment count
     */
    public int errorCount() {
        return errorCount;
    }

    /**
     * Returns closed segment count.
     *
     * @return closed segment count
     */
    public int closedCount() {
        return closedCount;
    }

    /**
     * Returns mapped but not currently loaded segment count.
     *
     * @return mapped but not currently loaded segment count
     */
    public int unloadedMappedSegmentCount() {
        return unloadedMappedSegmentCount;
    }

    /**
     * Returns total key count.
     *
     * @return total key count
     */
    public long totalKeys() {
        return totalKeys;
    }

    /**
     * Returns total cache key count.
     *
     * @return total cache key count
     */
    public long totalCacheKeys() {
        return totalCacheKeys;
    }

    /**
     * Returns total delta cache file count.
     *
     * @return total delta cache file count
     */
    public long totalDeltaCacheFiles() {
        return totalDeltaCacheFiles;
    }

    /**
     * Returns immutable per-segment runtime metrics.
     *
     * @return per-segment runtime metrics
     */
    public List<SegmentIndexSegmentRuntimeMetrics> runtimeMetrics() {
        return runtimeMetrics;
    }
}
