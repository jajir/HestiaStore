package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved baseline for runtime-tunable settings.
 */
public final class EffectiveIndexRuntimeTuningConfiguration {

    private final int maxSegmentsInCache;
    private final int segmentCacheKeyLimit;
    private final EffectiveIndexWritePathConfiguration writePath;

    EffectiveIndexRuntimeTuningConfiguration(final int maxSegmentsInCache,
            final int segmentCacheKeyLimit,
            final EffectiveIndexWritePathConfiguration writePath) {
        this.maxSegmentsInCache = Vldtn.requireGreaterThanZero(
                maxSegmentsInCache, "maxSegmentsInCache");
        this.segmentCacheKeyLimit = Vldtn.requireGreaterThanZero(
                segmentCacheKeyLimit, "segmentCacheKeyLimit");
        this.writePath = Vldtn.requireNonNull(writePath, "writePath");
    }

    public int maxSegmentsInCache() {
        return maxSegmentsInCache;
    }

    public int segmentCacheKeyLimit() {
        return segmentCacheKeyLimit;
    }

    public EffectiveIndexWritePathConfiguration writePath() {
        return writePath;
    }
}
