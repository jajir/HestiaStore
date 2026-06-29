package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;
import java.util.List;
import java.util.Objects;

/**
 * Segment metrics section inside an index report payload.
 */
public final class SegmentReportResponse {

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
    private final List<SegmentRuntimeReportResponse> runtimeMetrics;

    /**
     * Creates segment metrics.
     *
     * @param cacheKeyLimitPerSegment cache key limit per segment
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
    @ConstructorProperties({ "cacheKeyLimitPerSegment", "count",
            "readyCount", "maintenanceCount", "errorCount", "closedCount",
            "unloadedMappedSegmentCount", "totalKeys", "totalCacheKeys",
            "totalDeltaCacheFiles", "runtimeMetrics" })
    @SuppressWarnings("java:S107")
    public SegmentReportResponse(final int cacheKeyLimitPerSegment,
            final int count, final int readyCount, final int maintenanceCount,
            final int errorCount, final int closedCount,
            final int unloadedMappedSegmentCount,
            final long totalKeys, final long totalCacheKeys,
            final long totalDeltaCacheFiles,
            final List<SegmentRuntimeReportResponse> runtimeMetrics) {
        this.cacheKeyLimitPerSegment = cacheKeyLimitPerSegment;
        this.count = count;
        this.readyCount = readyCount;
        this.maintenanceCount = maintenanceCount;
        this.errorCount = errorCount;
        this.closedCount = closedCount;
        this.unloadedMappedSegmentCount = unloadedMappedSegmentCount;
        this.totalKeys = totalKeys;
        this.totalCacheKeys = totalCacheKeys;
        this.totalDeltaCacheFiles = totalDeltaCacheFiles;
        this.runtimeMetrics = List.copyOf(Objects.requireNonNull(
                runtimeMetrics, "runtimeMetrics"));
    }

    /**
     * Returns cache key limit per segment.
     *
     * @return cache key limit per segment
     */
    public int cacheKeyLimitPerSegment() {
        return cacheKeyLimitPerSegment;
    }

    /**
     * Returns mapped segment count.
     *
     * @return mapped segment count
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
     * Returns per-segment runtime metrics.
     *
     * @return per-segment runtime metrics
     */
    public List<SegmentRuntimeReportResponse> runtimeMetrics() {
        return runtimeMetrics;
    }
}
