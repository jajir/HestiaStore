package org.hestiastore.index.segment;

/**
 * Provide some basic statistic information about segment.
 * 
 * In number of keys in cache is counted even tombstones.
 * 
 * @author honza
 *
 */
public class SegmentStats {

    private final long numberOfKeysInDeltaCache;
    private final long numberOfKeysInIndex;
    private final long numberOfKeysInScarceIndex;
    private final long compactRequestCount;
    private final long flushRequestCount;

    /**
     * Creates a snapshot of segment key statistics.
     *
     * @param numberOfKeysInDeltaCache number of keys in delta cache
     * @param numberOfKeysInSegment number of keys in the main index
     * @param numberOfKeysInScarceIndex number of keys in the scarce index
     */
    public SegmentStats(final long numberOfKeysInDeltaCache,
            final long numberOfKeysInSegment,
            final long numberOfKeysInScarceIndex) {
        this(numberOfKeysInDeltaCache, numberOfKeysInSegment,
                numberOfKeysInScarceIndex, 0L, 0L);
    }

    /**
     * Creates a snapshot of segment key and maintenance statistics.
     *
     * @param numberOfKeysInDeltaCache number of keys in delta cache
     * @param numberOfKeysInSegment number of keys in the main index
     * @param numberOfKeysInScarceIndex number of keys in the scarce index
     * @param compactRequestCount compaction request count
     * @param flushRequestCount flush request count
     */
    public SegmentStats(final long numberOfKeysInDeltaCache,
            final long numberOfKeysInSegment,
            final long numberOfKeysInScarceIndex,
            final long compactRequestCount, final long flushRequestCount) {
        this.numberOfKeysInDeltaCache = numberOfKeysInDeltaCache;
        this.numberOfKeysInIndex = numberOfKeysInSegment;
        this.numberOfKeysInScarceIndex = numberOfKeysInScarceIndex;
        this.compactRequestCount = compactRequestCount;
        this.flushRequestCount = flushRequestCount;
    }

    /**
     * Returns the number of keys in the delta cache.
     *
     * @return delta cache key count
     */
    public long getNumberOfKeysInDeltaCache() {
        return numberOfKeysInDeltaCache;
    }

    /**
     * Returns the number of keys in the main index.
     *
     * @return index key count
     */
    public long getNumberOfKeysInSegment() {
        return numberOfKeysInIndex;
    }

    /**
     * Returns the total number of keys in the segment (delta + index).
     *
     * @return total key count
     */
    public long getNumberOfKeys() {
        return getNumberOfKeysInDeltaCache() + getNumberOfKeysInSegment();
    }

    /**
     * Returns the number of keys in the scarce index.
     *
     * @return scarce index key count
     */
    public long getNumberOfKeysInScarceIndex() {
        return numberOfKeysInScarceIndex;
    }

    /**
     * Returns the number of compaction requests.
     *
     * @return compaction request count
     */
    public long getCompactRequestCount() {
        return compactRequestCount;
    }

    /**
     * Returns the number of flush requests.
     *
     * @return flush request count
     */
    public long getFlushRequestCount() {
        return flushRequestCount;
    }
}
