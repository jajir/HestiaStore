package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Supported runtime-tunable setting keys.
 */
enum RuntimeSettingKey {
    /** Segment registry cache capacity. */
    MAX_NUMBER_OF_SEGMENTS_IN_CACHE(
            RuntimeTuningField.SEGMENT_CACHED_SEGMENT_LIMIT),
    /** Segment cache pressure/compaction threshold. */
    MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE(
            RuntimeTuningField.SEGMENT_CACHE_KEY_LIMIT),
    /** Steady-state segment write-cache key limit. */
    SEGMENT_WRITE_CACHE_KEY_LIMIT(
            RuntimeTuningField.WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT),
    /** Maintenance-time segment write-cache key limit. */
    SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE(
            RuntimeTuningField.WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE),
    /** Buffered key limit across the full index overlay. */
    INDEX_BUFFERED_WRITE_KEY_LIMIT(
            RuntimeTuningField.WRITE_PATH_INDEX_BUFFERED_WRITE_KEY_LIMIT),
    /** Split threshold for a single routed segment. */
    SEGMENT_SPLIT_KEY_THRESHOLD(
            RuntimeTuningField.WRITE_PATH_SEGMENT_SPLIT_KEY_THRESHOLD);

    private final RuntimeTuningField field;

    RuntimeSettingKey(final RuntimeTuningField field) {
        this.field = field;
    }

    RuntimeTuningField field() {
        return field;
    }
}
