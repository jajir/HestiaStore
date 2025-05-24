package com.hestiastore.index.segment;

/**
 * Provide some basic statistic information about segment.
 * 
 * In number of keys in cache is counted even tombstones.
 * 
 * @author honza
 *
 */
class SegmentStats {

    private final long numberOfKeysInDeltaCache;
    private final long numberOfKeysInIndex;
    private final long numberOfKeysInScarceIndex;

    SegmentStats(final long numberOfKeysInDeltaCache,
            final long numberOfKeysInSegment,
            final long numberOfKeysInScarceIndex) {
        this.numberOfKeysInDeltaCache = numberOfKeysInDeltaCache;
        this.numberOfKeysInIndex = numberOfKeysInSegment;
        this.numberOfKeysInScarceIndex = numberOfKeysInScarceIndex;
    }

    public long getNumberOfKeysInDeltaCache() {
        return numberOfKeysInDeltaCache;
    }

    public long getNumberOfKeysInSegment() {
        return numberOfKeysInIndex;
    }

    public long getNumberOfKeys() {
        return getNumberOfKeysInDeltaCache() + getNumberOfKeysInSegment();
    }

    public long getNumberOfKeysInScarceIndex() {
        return numberOfKeysInScarceIndex;
    }
}
