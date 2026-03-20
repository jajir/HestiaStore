package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Immutable runtime metrics snapshot for one segment.
 */
@SuppressWarnings({ "java:S6206", "java:S107" })
public final class SegmentRuntimeSnapshot {

    private final SegmentId segmentId;
    private final SegmentState state;
    private final long numberOfKeysInDeltaCache;
    private final long numberOfKeysInSegment;
    private final long numberOfKeysInScarceIndex;
    private final long numberOfKeysInSegmentCache;
    private final int numberOfKeysInWriteCache;
    private final int numberOfDeltaCacheFiles;
    private final long numberOfCompacts;
    private final long numberOfFlushes;
    private final long bloomFilterRequestCount;
    private final long bloomFilterRefusedCount;
    private final long bloomFilterPositiveCount;
    private final long bloomFilterFalsePositiveCount;

    /**
     * Creates validated runtime metrics snapshot.
     *
     * @param segmentId segment identifier
     * @param state segment state
     * @param numberOfKeysInDeltaCache number of keys in delta cache
     * @param numberOfKeysInSegment number of keys in segment index
     * @param numberOfKeysInScarceIndex number of keys in scarce index
     * @param numberOfKeysInSegmentCache number of keys in in-memory segment cache
     * @param numberOfKeysInWriteCache number of keys in write cache
     * @param numberOfDeltaCacheFiles number of delta cache files
     * @param numberOfCompacts number of compact executions/requests
     * @param numberOfFlushes number of flush executions/requests
     * @param bloomFilterRequestCount bloom request count
     * @param bloomFilterRefusedCount bloom refused count
     * @param bloomFilterPositiveCount bloom positive count
     * @param bloomFilterFalsePositiveCount bloom false-positive count
     */
    public SegmentRuntimeSnapshot(final SegmentId segmentId,
            final SegmentState state, final long numberOfKeysInDeltaCache,
            final long numberOfKeysInSegment,
            final long numberOfKeysInScarceIndex,
            final long numberOfKeysInSegmentCache,
            final int numberOfKeysInWriteCache,
            final int numberOfDeltaCacheFiles, final long numberOfCompacts,
            final long numberOfFlushes, final long bloomFilterRequestCount,
            final long bloomFilterRefusedCount,
            final long bloomFilterPositiveCount,
            final long bloomFilterFalsePositiveCount) {
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
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
        requireNotNegative(numberOfCompacts, "numberOfCompacts");
        requireNotNegative(numberOfFlushes, "numberOfFlushes");
        requireNotNegative(bloomFilterRequestCount, "bloomFilterRequestCount");
        requireNotNegative(bloomFilterRefusedCount, "bloomFilterRefusedCount");
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
        this.numberOfCompacts = numberOfCompacts;
        this.numberOfFlushes = numberOfFlushes;
        this.bloomFilterRequestCount = bloomFilterRequestCount;
        this.bloomFilterRefusedCount = bloomFilterRefusedCount;
        this.bloomFilterPositiveCount = bloomFilterPositiveCount;
        this.bloomFilterFalsePositiveCount = bloomFilterFalsePositiveCount;
    }

    private static void requireNotNegative(final long value,
            final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    public SegmentId getSegmentId() {
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

    public long getNumberOfKeys() {
        return numberOfKeysInDeltaCache + numberOfKeysInSegment;
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

    public long getNumberOfCompacts() {
        return numberOfCompacts;
    }

    public long getNumberOfFlushes() {
        return numberOfFlushes;
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
