package org.hestiastore.index.segmentindex.runtimeconfiguration;

/**
 * Supported runtime-tunable setting keys.
 */
public enum RuntimeSettingKey {
    /** Segment registry cache capacity. */
    MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
    /** Segment cache pressure/compaction threshold. */
    MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
    /** Active partition in-memory threshold. */
    MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
    /** Immutable run queue depth per partition. */
    MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
    /** Buffered key limit inside a single partition. */
    MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
    /** Buffered key limit across the full index overlay. */
    MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
    /** Split/drain threshold for a single routed partition. */
    MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT
}
