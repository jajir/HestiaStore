package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Builder section for parsed persisted chunk page cache settings.
 */
public final class IndexChunkStoreCacheConfigurationBuilder {

    private Integer pageLimit;

    IndexChunkStoreCacheConfigurationBuilder() {
    }

    /**
     * Sets the max parsed page count, or {@code 0} to disable the cache.
     *
     * @param value max cached pages
     * @return this section builder
     */
    public IndexChunkStoreCacheConfigurationBuilder pageLimit(
            final Integer value) {
        this.pageLimit = value;
        return this;
    }

    IndexChunkStoreCacheConfiguration build() {
        return new IndexChunkStoreCacheConfiguration(pageLimit);
    }
}
