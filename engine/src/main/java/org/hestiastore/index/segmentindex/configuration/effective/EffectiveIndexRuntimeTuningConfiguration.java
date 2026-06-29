package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved baseline for runtime-tunable settings.
 */
public final class EffectiveIndexRuntimeTuningConfiguration {

    private final int maxSegmentsInCache;
    private final int segmentCacheKeyLimit;
    private final EffectiveIndexWritePathConfiguration writePath;
    private final EffectiveIndexChunkStoreCacheConfiguration chunkStoreCache;

    EffectiveIndexRuntimeTuningConfiguration(final int maxSegmentsInCache,
            final int segmentCacheKeyLimit,
            final EffectiveIndexWritePathConfiguration writePath,
            final EffectiveIndexChunkStoreCacheConfiguration chunkStoreCache) {
        this.maxSegmentsInCache = Vldtn.requireGreaterThanZero(
                maxSegmentsInCache, "maxSegmentsInCache");
        this.segmentCacheKeyLimit = Vldtn.requireGreaterThanZero(
                segmentCacheKeyLimit, "segmentCacheKeyLimit");
        this.writePath = Vldtn.requireNonNull(writePath, "writePath");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
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

    public EffectiveIndexChunkStoreCacheConfiguration chunkStoreCache() {
        return chunkStoreCache;
    }
}
