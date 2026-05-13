package org.hestiastore.index.segmentindex.configuration.user;

/**
 * Builder section for parsed persisted chunk page cache settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexChunkStoreCacheConfigurationBuilder<K, V> {

    private Integer pageLimit;

    IndexChunkStoreCacheConfigurationBuilder() {
    }

    /**
     * Sets the max parsed page count, or {@code 0} to disable the cache.
     *
     * @param value max cached pages
     * @return this section builder
     */
    public IndexChunkStoreCacheConfigurationBuilder<K, V> pageLimit(
            final Integer value) {
        this.pageLimit = value;
        return this;
    }

    IndexChunkStoreCacheConfiguration build() {
        return new IndexChunkStoreCacheConfiguration(pageLimit);
    }
}
