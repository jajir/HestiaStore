package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;

/**
 * Typed runtime segment tuning snapshot.
 */
public final class RuntimeSegmentTuningSnapshot {

    private final int cacheKeyLimit;
    private final int cachedSegmentLimit;

    RuntimeSegmentTuningSnapshot(final int cacheKeyLimit,
            final int cachedSegmentLimit) {
        this.cacheKeyLimit = Vldtn.requireGreaterThanZero(cacheKeyLimit,
                "cacheKeyLimit");
        this.cachedSegmentLimit = Vldtn.requireGreaterThanZero(
                cachedSegmentLimit, "cachedSegmentLimit");
    }

    public int cacheKeyLimit() {
        return cacheKeyLimit;
    }

    public int cachedSegmentLimit() {
        return cachedSegmentLimit;
    }
}
