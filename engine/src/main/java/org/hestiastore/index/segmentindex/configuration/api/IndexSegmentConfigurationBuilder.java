package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Builder section for segment sizing and segment cache settings.
 */
public final class IndexSegmentConfigurationBuilder {

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
    public IndexSegmentConfigurationBuilder maxKeys(
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
    public IndexSegmentConfigurationBuilder chunkKeyLimit(
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
    public IndexSegmentConfigurationBuilder cacheKeyLimit(
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
    public IndexSegmentConfigurationBuilder cachedSegmentLimit(
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
    public IndexSegmentConfigurationBuilder deltaCacheFileLimit(
            final Integer value) {
        this.deltaCacheFileLimit = value;
        return this;
    }

    IndexSegmentConfiguration build() {
        return new IndexSegmentConfiguration(maxKeys, chunkKeyLimit,
                cacheKeyLimit, cachedSegmentLimit, deltaCacheFileLimit);
    }
}
