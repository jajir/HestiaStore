package org.hestiastore.index.segmentindex;

/**
 * Immutable segment sizing and cache settings view.
 */
public final class IndexSegmentConfiguration {

    private final Integer maxKeys;
    private final Integer chunkKeyLimit;
    private final Integer cacheKeyLimit;
    private final Integer cachedSegmentLimit;
    private final Integer deltaCacheFileLimit;

    public IndexSegmentConfiguration(final Integer maxKeys,
            final Integer chunkKeyLimit, final Integer cacheKeyLimit,
            final Integer cachedSegmentLimit,
            final Integer deltaCacheFileLimit) {
        this.maxKeys = maxKeys;
        this.chunkKeyLimit = chunkKeyLimit;
        this.cacheKeyLimit = cacheKeyLimit;
        this.cachedSegmentLimit = cachedSegmentLimit;
        this.deltaCacheFileLimit = deltaCacheFileLimit;
    }

    public Integer maxKeys() {
        return maxKeys;
    }

    public Integer chunkKeyLimit() {
        return chunkKeyLimit;
    }

    public Integer cacheKeyLimit() {
        return cacheKeyLimit;
    }

    public Integer cachedSegmentLimit() {
        return cachedSegmentLimit;
    }

    public Integer deltaCacheFileLimit() {
        return deltaCacheFileLimit;
    }
}
