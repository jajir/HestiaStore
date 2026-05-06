package org.hestiastore.index.segmentindex.tuning;

/**
 * Supported runtime-tunable setting keys.
 */
public enum RuntimeSettingKey {
    /** Segment registry cache capacity. */
    MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
    /** Segment cache pressure/compaction threshold. */
    MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
    /** Steady-state segment write-cache key limit. */
    SEGMENT_WRITE_CACHE_KEY_LIMIT,
    /** Maintenance-time segment write-cache key limit. */
    SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
    /** Buffered key limit across the full index overlay. */
    INDEX_BUFFERED_WRITE_KEY_LIMIT,
    /** Split threshold for a single routed segment. */
    SEGMENT_SPLIT_KEY_THRESHOLD
}
