package com.coroptis.index.segment;

/**
 * Provide some basic statistic information about segment.
 * 
 * In number of keys in cache is counted even tombstones.
 * 
 * @author honza
 *
 */
class SegmentStats {

    private final long numberOfKeysInCache;
    private final long numberOfKeysInIndex;
    private final long numberOfKeysInScarceIndex;

    SegmentStats(final long numberOfKeysInCache, final long numberOfKeysInIndex,
            final long numberOfKeysInScarceIndex) {
        this.numberOfKeysInCache = numberOfKeysInCache;
        this.numberOfKeysInIndex = numberOfKeysInIndex;
        this.numberOfKeysInScarceIndex = numberOfKeysInScarceIndex;
    }

    public long getNumberOfKeysInCache() {
        return numberOfKeysInCache;
    }

    public long getNumberOfKeysInIndex() {
        return numberOfKeysInIndex;
    }

    public long getNumberOfKeys() {
        return getNumberOfKeysInCache() + getNumberOfKeysInIndex();
    }

    public long getNumberOfKeysInScarceIndex() {
        return numberOfKeysInScarceIndex;
    }
}
