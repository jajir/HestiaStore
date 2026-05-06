package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for direct-to-segment write-path limits.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexWritePathConfigurationBuilder<K, V> {

    private static final String PROPERTY_INDEX_BUFFERED_WRITE_KEY_LIMIT =
            "indexBufferedWriteKeyLimit";
    private static final String PROPERTY_SEGMENT_WRITE_CACHE_KEY_LIMIT =
            "segmentWriteCacheKeyLimit";
    private static final String PROPERTY_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE =
            "segmentWriteCacheKeyLimitDuringMaintenance";

    private Integer segmentWriteCacheKeyLimit;
    private Integer maintenanceWriteCacheKeyLimit;
    private Integer indexBufferedWriteKeyLimit;
    private Integer segmentSplitKeyThreshold;

    IndexWritePathConfigurationBuilder() {
    }

    /**
     * Sets steady-state segment write-cache key limit.
     *
     * @param value segment write-cache key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> segmentWriteCacheKeyLimit(
            final Integer value) {
        this.segmentWriteCacheKeyLimit = value;
        return this;
    }

    /**
     * Sets maintenance-time segment write-cache key limit.
     *
     * @param value maintenance write-cache key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> maintenanceWriteCacheKeyLimit(
            final Integer value) {
        this.maintenanceWriteCacheKeyLimit = value;
        return this;
    }

    /**
     * Sets index-wide buffered write key limit.
     *
     * @param value index-wide buffered key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> indexBufferedWriteKeyLimit(
            final Integer value) {
        this.indexBufferedWriteKeyLimit = value;
        return this;
    }

    /**
     * Sets segment split key threshold.
     *
     * @param value segment split threshold
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> segmentSplitKeyThreshold(
            final Integer value) {
        this.segmentSplitKeyThreshold = value;
        return this;
    }

    Integer segmentSplitKeyThreshold() {
        return segmentSplitKeyThreshold;
    }

    IndexWritePathConfiguration build(final Integer segmentMaxKeys,
            final Integer cachedSegmentLimit) {
        final Integer effectiveSegmentSplitKeyThreshold = segmentSplitKeyThreshold == null
                ? segmentMaxKeys
                : segmentSplitKeyThreshold;
        final Integer effectiveMaintenanceWriteCacheKeyLimit =
                resolveEffectiveMaintenanceWriteCacheKeyLimit();
        final Integer effectiveIndexBufferedWriteKeyLimit =
                resolveEffectiveIndexBufferedWriteKeyLimit(
                        effectiveMaintenanceWriteCacheKeyLimit,
                        cachedSegmentLimit);
        return new IndexWritePathConfiguration(segmentWriteCacheKeyLimit,
                effectiveMaintenanceWriteCacheKeyLimit,
                effectiveIndexBufferedWriteKeyLimit,
                effectiveSegmentSplitKeyThreshold);
    }

    private Integer resolveEffectiveMaintenanceWriteCacheKeyLimit() {
        if (maintenanceWriteCacheKeyLimit == null
                && segmentWriteCacheKeyLimit != null) {
            return Math.max(
                    (int) Math.ceil(segmentWriteCacheKeyLimit * 1.4),
                    segmentWriteCacheKeyLimit + 1);
        }
        if (maintenanceWriteCacheKeyLimit == null) {
            return null;
        }
        if (segmentWriteCacheKeyLimit == null) {
            return Vldtn.requireGreaterThanZero(
                    maintenanceWriteCacheKeyLimit,
                    PROPERTY_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE);
        }
        if (maintenanceWriteCacheKeyLimit <= segmentWriteCacheKeyLimit) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be greater than '%s'",
                    PROPERTY_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                    PROPERTY_SEGMENT_WRITE_CACHE_KEY_LIMIT));
        }
        return maintenanceWriteCacheKeyLimit;
    }

    private Integer resolveEffectiveIndexBufferedWriteKeyLimit(
            final Integer effectiveMaintenanceWriteCacheKeyLimit,
            final Integer cachedSegmentLimit) {
        if (indexBufferedWriteKeyLimit != null) {
            if (effectiveMaintenanceWriteCacheKeyLimit != null
                    && indexBufferedWriteKeyLimit
                            .intValue() < effectiveMaintenanceWriteCacheKeyLimit
                                    .intValue()) {
                throw new IllegalArgumentException(String.format(
                        "Property '%s' must be greater than or equal to '%s'",
                        PROPERTY_INDEX_BUFFERED_WRITE_KEY_LIMIT,
                        PROPERTY_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE));
            }
            return indexBufferedWriteKeyLimit;
        }
        if (effectiveMaintenanceWriteCacheKeyLimit == null) {
            return null;
        }
        final int segmentCount = cachedSegmentLimit == null
                ? IndexConfigurationContract.DEFAULT_CACHED_SEGMENT_LIMIT
                : cachedSegmentLimit.intValue();
        return Integer.valueOf(Math.max(
                effectiveMaintenanceWriteCacheKeyLimit.intValue(),
                effectiveMaintenanceWriteCacheKeyLimit.intValue()
                        * Math.max(1, segmentCount)));
    }
}
