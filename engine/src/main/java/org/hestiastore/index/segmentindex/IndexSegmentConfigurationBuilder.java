package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for segment sizing and segment cache settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexSegmentConfigurationBuilder<K, V> {

    private final IndexConfigurationBuilder<K, V> builder;

    IndexSegmentConfigurationBuilder(
            final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Sets maximum keys in one segment.
     *
     * @param value max keys in one segment
     * @return this section builder
     */
    public IndexSegmentConfigurationBuilder<K, V> maxKeys(
            final Integer value) {
        builder.setSegmentMaxKeys(value);
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
        builder.setSegmentChunkKeyLimit(value);
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
        builder.setSegmentCacheKeyLimit(value);
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
        builder.setCachedSegmentLimit(value);
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
        builder.setSegmentDeltaCacheFileLimit(value);
        return this;
    }
}
