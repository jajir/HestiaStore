package org.hestiastore.index.segmentindex;

/**
 * Immutable runtime-tunable settings view derived from index configuration.
 */
public final class IndexRuntimeTuningConfiguration {

    private final Integer maxSegmentsInCache;
    private final Integer segmentCacheKeyLimit;
    private final IndexWritePathConfiguration writePath;
    private final Integer legacyImmutableRunLimit;

    IndexRuntimeTuningConfiguration(final Integer maxSegmentsInCache,
            final Integer segmentCacheKeyLimit,
            final IndexWritePathConfiguration writePath,
            final Integer legacyImmutableRunLimit) {
        this.maxSegmentsInCache = maxSegmentsInCache;
        this.segmentCacheKeyLimit = segmentCacheKeyLimit;
        this.writePath = writePath;
        this.legacyImmutableRunLimit = legacyImmutableRunLimit;
    }

    public Integer maxSegmentsInCache() {
        return maxSegmentsInCache;
    }

    public Integer segmentCacheKeyLimit() {
        return segmentCacheKeyLimit;
    }

    public IndexWritePathConfiguration writePath() {
        return writePath;
    }

    public Integer legacyImmutableRunLimit() {
        return legacyImmutableRunLimit;
    }
}
