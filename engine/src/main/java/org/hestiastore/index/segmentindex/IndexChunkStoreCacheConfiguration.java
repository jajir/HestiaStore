package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Runtime configuration for the parsed persisted chunk page cache.
 */
public final class IndexChunkStoreCacheConfiguration {

    private final Integer pageLimit;

    public IndexChunkStoreCacheConfiguration(final Integer pageLimit) {
        this.pageLimit = pageLimit == null ? null
                : Vldtn.requireGreaterThanOrEqualToZero(pageLimit,
                        "pageLimit");
    }

    /**
     * Returns the max parsed page count, or {@code 0} to disable caching.
     *
     * @return nullable requested page limit
     */
    public Integer pageLimit() {
        return pageLimit;
    }
}
