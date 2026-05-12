package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Public field identifiers for runtime tuning values.
 */
public enum RuntimeTuningField {
    /** Segment registry cache capacity. */
    SEGMENT_CACHED_SEGMENT_LIMIT("segment.cachedSegmentLimit",
            RuntimeTuningValueType.INT),
    /** Segment cache pressure/compaction threshold. */
    SEGMENT_CACHE_KEY_LIMIT("segment.cacheKeyLimit",
            RuntimeTuningValueType.INT),
    /** Steady-state segment write-cache key limit. */
    WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT(
            "writePath.segmentWriteCacheKeyLimit", RuntimeTuningValueType.INT),
    /** Maintenance-time segment write-cache key limit. */
    WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE(
            "writePath.segmentWriteCacheKeyLimitDuringMaintenance",
            RuntimeTuningValueType.INT),
    /** Buffered key limit across the full index overlay. */
    WRITE_PATH_INDEX_BUFFERED_WRITE_KEY_LIMIT(
            "writePath.indexBufferedWriteKeyLimit",
            RuntimeTuningValueType.INT),
    /** Split threshold for a single routed segment. */
    WRITE_PATH_SEGMENT_SPLIT_KEY_THRESHOLD(
            "writePath.segmentSplitKeyThreshold", RuntimeTuningValueType.INT),
    /** Parsed persisted chunk page cache capacity. */
    CHUNK_STORE_CACHE_PAGE_LIMIT("chunkStoreCache.pageLimit",
            RuntimeTuningValueType.INT);

    private final String path;
    private final RuntimeTuningValueType valueType;

    RuntimeTuningField(final String path,
            final RuntimeTuningValueType valueType) {
        this.path = path;
        this.valueType = valueType;
    }

    public String path() {
        return path;
    }

    public RuntimeTuningValueType valueType() {
        return valueType;
    }
}
