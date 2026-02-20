package org.hestiastore.index.control.model;

/**
 * Supported runtime-tunable setting keys.
 */
public enum RuntimeSettingKey {
    /** Segment registry cache capacity. */
    MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
    /** Segment cache pressure/compaction threshold. */
    MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
    /** Write-cache flush threshold. */
    MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE,
    /** Write-cache threshold allowed while maintenance is running. */
    MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE
}
