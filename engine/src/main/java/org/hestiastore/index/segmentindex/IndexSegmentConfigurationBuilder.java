package org.hestiastore.index.segmentindex;

/**
 * Builder section for segment sizing and segment cache settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexSegmentConfigurationBuilder<K, V> {

    private Integer maxKeys;
    private Integer chunkKeyLimit;
    private Integer cacheKeyLimit;
    private Integer cachedSegmentLimit;
    private Integer deltaCacheFileLimit;

    IndexSegmentConfigurationBuilder() {
    }

    /**
     * Sets maximum keys in one segment.
     *
     * @param value max keys in one segment
     * @return this section builder
     */
    public IndexSegmentConfigurationBuilder<K, V> maxKeys(
            final Integer value) {
        this.maxKeys = value;
        return this;
    }

    /**
     * Sets max keys per on-disk segment chunk.
     *
     * @param value max keys per chunk
     * @return this section builder
     */
    public IndexSegmentConfigurationBuilder<K, V> chunkKeyLimit(
            final Integer value) {
        this.chunkKeyLimit = value;
        return this;
    }

    /**
     * Sets max keys retained in a segment cache.
     *
     * @param value segment cache key limit
     * @return this section builder
     */
    public IndexSegmentConfigurationBuilder<K, V> cacheKeyLimit(
            final Integer value) {
        this.cacheKeyLimit = value;
        return this;
    }

    /**
     * Sets number of segments retained in the index-level cache.
     *
     * @param value cached segment limit
     * @return this section builder
     */
    public IndexSegmentConfigurationBuilder<K, V> cachedSegmentLimit(
            final Integer value) {
        this.cachedSegmentLimit = value;
        return this;
    }

    /**
     * Sets max delta cache files before maintenance is triggered.
     *
     * @param value delta cache file limit
     * @return this section builder
     */
    public IndexSegmentConfigurationBuilder<K, V> deltaCacheFileLimit(
            final Integer value) {
        this.deltaCacheFileLimit = value;
        return this;
    }

    Integer resolveEffectiveMaxKeys(final Integer segmentSplitKeyThreshold) {
        if (maxKeys != null) {
            return maxKeys;
        }
        return segmentSplitKeyThreshold;
    }

    IndexSegmentConfiguration build(final Integer effectiveMaxKeys) {
        final Integer effectiveDeltaCacheFileLimit = deltaCacheFileLimit == null
                ? IndexConfigurationContract.DEFAULT_DELTA_CACHE_FILE_LIMIT
                : deltaCacheFileLimit;
        return new IndexSegmentConfiguration(effectiveMaxKeys, chunkKeyLimit,
                cacheKeyLimit, cachedSegmentLimit,
                effectiveDeltaCacheFileLimit);
    }
}
