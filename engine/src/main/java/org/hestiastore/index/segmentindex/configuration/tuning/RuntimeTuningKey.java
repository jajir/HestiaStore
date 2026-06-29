package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Supported runtime-tunable integer setting keys.
 */
public enum RuntimeTuningKey {
    /** Segment registry cache capacity. */
    MAX_NUMBER_OF_SEGMENTS_IN_CACHE("segment.cachedSegmentLimit"),
    /** Segment cache pressure/compaction threshold. */
    MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE("segment.cacheKeyLimit"),
    /** Steady-state segment write-cache key limit. */
    SEGMENT_WRITE_CACHE_KEY_LIMIT("writePath.segmentWriteCacheKeyLimit"),
    /** Maintenance-time segment write-cache key limit. */
    SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE(
            "writePath.segmentWriteCacheKeyLimitDuringMaintenance"),
    /** Buffered key limit across the full index overlay. */
    INDEX_BUFFERED_WRITE_KEY_LIMIT("writePath.indexBufferedWriteKeyLimit"),
    /** Split threshold for a single routed segment. */
    SEGMENT_SPLIT_KEY_THRESHOLD("writePath.segmentSplitKeyThreshold"),
    /** Parsed persisted chunk page cache capacity. */
    CHUNK_STORE_CACHE_PAGE_LIMIT("chunkStoreCache.pageLimit");

    private final String path;

    RuntimeTuningKey(final String path) {
        this.path = path;
    }

    /**
     * Returns the external configuration path for this setting.
     *
     * @return setting path
     */
    public String path() {
        return path;
    }
}
