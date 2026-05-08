package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved segment sizing and cache settings.
 */
public final class EffectiveIndexSegmentConfiguration {

    private final int maxKeys;
    private final int chunkKeyLimit;
    private final int cacheKeyLimit;
    private final int cachedSegmentLimit;
    private final int deltaCacheFileLimit;

    public EffectiveIndexSegmentConfiguration(final int maxKeys,
            final int chunkKeyLimit, final int cacheKeyLimit,
            final int cachedSegmentLimit, final int deltaCacheFileLimit) {
        this.maxKeys = Vldtn.requireGreaterThanZero(maxKeys, "maxKeys");
        this.chunkKeyLimit = Vldtn.requireGreaterThanZero(chunkKeyLimit,
                "chunkKeyLimit");
        this.cacheKeyLimit = Vldtn.requireGreaterThanZero(cacheKeyLimit,
                "cacheKeyLimit");
        this.cachedSegmentLimit = Vldtn.requireGreaterThanZero(
                cachedSegmentLimit, "cachedSegmentLimit");
        this.deltaCacheFileLimit = Vldtn.requireGreaterThanZero(
                deltaCacheFileLimit, "deltaCacheFileLimit");
    }

    public int maxKeys() {
        return maxKeys;
    }

    public int chunkKeyLimit() {
        return chunkKeyLimit;
    }

    public int cacheKeyLimit() {
        return cacheKeyLimit;
    }

    public int cachedSegmentLimit() {
        return cachedSegmentLimit;
    }

    public int deltaCacheFileLimit() {
        return deltaCacheFileLimit;
    }
}
