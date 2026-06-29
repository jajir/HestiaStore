package org.hestiastore.index.segmentindex.monitoring.model;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentState;

/**
 * User-facing runtime metrics for one mapped segment.
 */
public final class SegmentIndexSegmentRuntimeMetrics {

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
     * Creates segment runtime metrics.
     *
     * @param segmentId segment identifier
     * @param state segment state
     * @param numberOfKeysInDeltaCache number of keys in delta cache
     * @param numberOfKeysInSegment number of keys in segment files
     * @param numberOfKeysInScarceIndex number of keys in scarce index
     * @param numberOfKeysInSegmentCache number of keys in segment cache
     * @param numberOfKeysInWriteCache number of keys in write cache
     * @param numberOfDeltaCacheFiles number of delta cache files
     * @param compactRequestCount compact request count
     * @param flushRequestCount flush request count
     * @param bloomFilterRequestCount Bloom filter request count
     * @param bloomFilterRefusedCount Bloom filter refused count
     * @param bloomFilterPositiveCount Bloom filter positive count
     * @param bloomFilterFalsePositiveCount Bloom filter false-positive count
     */
    @SuppressWarnings("java:S107")
    public SegmentIndexSegmentRuntimeMetrics(final String segmentId,
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
        this.numberOfKeysInDeltaCache = MetricModelValidation.nonNegative(
                numberOfKeysInDeltaCache, "numberOfKeysInDeltaCache");
        this.numberOfKeysInSegment = MetricModelValidation.nonNegative(
                numberOfKeysInSegment, "numberOfKeysInSegment");
        this.numberOfKeysInScarceIndex = MetricModelValidation.nonNegative(
                numberOfKeysInScarceIndex, "numberOfKeysInScarceIndex");
        this.numberOfKeysInSegmentCache = MetricModelValidation.nonNegative(
                numberOfKeysInSegmentCache, "numberOfKeysInSegmentCache");
        this.numberOfKeysInWriteCache = MetricModelValidation.nonNegative(
                numberOfKeysInWriteCache, "numberOfKeysInWriteCache");
        this.numberOfDeltaCacheFiles = MetricModelValidation.nonNegative(
                numberOfDeltaCacheFiles, "numberOfDeltaCacheFiles");
        this.compactRequestCount = MetricModelValidation.nonNegative(
                compactRequestCount, "compactRequestCount");
        this.flushRequestCount = MetricModelValidation.nonNegative(
                flushRequestCount, "flushRequestCount");
        this.bloomFilterRequestCount = MetricModelValidation.nonNegative(
                bloomFilterRequestCount, "bloomFilterRequestCount");
        this.bloomFilterRefusedCount = MetricModelValidation.nonNegative(
                bloomFilterRefusedCount, "bloomFilterRefusedCount");
        this.bloomFilterPositiveCount = MetricModelValidation.nonNegative(
                bloomFilterPositiveCount, "bloomFilterPositiveCount");
        this.bloomFilterFalsePositiveCount = MetricModelValidation.nonNegative(
                bloomFilterFalsePositiveCount,
                "bloomFilterFalsePositiveCount");
    }

    /**
     * Returns the segment identifier.
     *
     * @return segment identifier
     */
    public String segmentId() {
        return segmentId;
    }

    /**
     * Returns the segment state.
     *
     * @return segment state
     */
    public SegmentState state() {
        return state;
    }

    /**
     * Returns number of keys in delta cache.
     *
     * @return delta-cache key count
     */
    public long numberOfKeysInDeltaCache() {
        return numberOfKeysInDeltaCache;
    }

    /**
     * Returns number of keys in segment files.
     *
     * @return segment key count
     */
    public long numberOfKeysInSegment() {
        return numberOfKeysInSegment;
    }

    /**
     * Returns number of keys in scarce index.
     *
     * @return scarce-index key count
     */
    public long numberOfKeysInScarceIndex() {
        return numberOfKeysInScarceIndex;
    }

    /**
     * Returns number of keys in segment cache.
     *
     * @return segment-cache key count
     */
    public long numberOfKeysInSegmentCache() {
        return numberOfKeysInSegmentCache;
    }

    /**
     * Returns number of keys in write cache.
     *
     * @return write-cache key count
     */
    public int numberOfKeysInWriteCache() {
        return numberOfKeysInWriteCache;
    }

    /**
     * Returns number of delta cache files.
     *
     * @return delta cache file count
     */
    public int numberOfDeltaCacheFiles() {
        return numberOfDeltaCacheFiles;
    }

    /**
     * Returns compact request count.
     *
     * @return compact request count
     */
    public long compactRequestCount() {
        return compactRequestCount;
    }

    /**
     * Returns flush request count.
     *
     * @return flush request count
     */
    public long flushRequestCount() {
        return flushRequestCount;
    }

    /**
     * Returns Bloom filter request count.
     *
     * @return Bloom filter request count
     */
    public long bloomFilterRequestCount() {
        return bloomFilterRequestCount;
    }

    /**
     * Returns Bloom filter refused count.
     *
     * @return Bloom filter refused count
     */
    public long bloomFilterRefusedCount() {
        return bloomFilterRefusedCount;
    }

    /**
     * Returns Bloom filter positive count.
     *
     * @return Bloom filter positive count
     */
    public long bloomFilterPositiveCount() {
        return bloomFilterPositiveCount;
    }

    /**
     * Returns Bloom filter false-positive count.
     *
     * @return Bloom filter false-positive count
     */
    public long bloomFilterFalsePositiveCount() {
        return bloomFilterFalsePositiveCount;
    }
}
