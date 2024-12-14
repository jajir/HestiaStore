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

    private final long numberOfKeysInDeltaCache;
    private final long numberOfKeysInIndex;
    private final long numberOfKeysInScarceIndex;

    SegmentStats(final long numberOfKeysInDeltaCache, final long numberOfKeysInIndex,
            final long numberOfKeysInScarceIndex) {
        this.numberOfKeysInDeltaCache = numberOfKeysInDeltaCache;
        this.numberOfKeysInIndex = numberOfKeysInIndex;
        this.numberOfKeysInScarceIndex = numberOfKeysInScarceIndex;
    }

    public long getNumberOfKeysInDeltaCache() {
        return numberOfKeysInDeltaCache;
    }

    public long getNumberOfKeysInIndex() {
        return numberOfKeysInIndex;
    }

    public long getNumberOfKeys() {
        return getNumberOfKeysInDeltaCache() + getNumberOfKeysInIndex();
    }

    public long getNumberOfKeysInScarceIndex() {
        return numberOfKeysInScarceIndex;
    }
}
