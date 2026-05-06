package org.hestiastore.index.segmentindex;

/**
 * Immutable runtime-tunable settings view derived from index configuration.
 */
public final class IndexRuntimeTuningConfiguration {

    private final Integer maxSegmentsInCache;
    private final Integer segmentCacheKeyLimit;
    private final IndexWritePathConfiguration writePath;

    IndexRuntimeTuningConfiguration(final Integer maxSegmentsInCache,
            final Integer segmentCacheKeyLimit,
            final IndexWritePathConfiguration writePath) {
        this.maxSegmentsInCache = maxSegmentsInCache;
        this.segmentCacheKeyLimit = segmentCacheKeyLimit;
        this.writePath = writePath;
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
}
