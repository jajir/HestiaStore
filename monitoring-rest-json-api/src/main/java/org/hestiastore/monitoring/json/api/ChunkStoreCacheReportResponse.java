package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Chunk-store cache metrics section inside an index report payload.
 */
public final class ChunkStoreCacheReportResponse {

    private final int pageLimit;
    private final int pageCount;
    private final long entryCount;
    private final long hitCount;
    private final long missCount;
    private final long loadCount;
    private final long evictionCount;
    private final long invalidationCount;

    /**
     * Creates chunk-store cache metrics.
     *
     * @param pageLimit cache page limit
     * @param pageCount current page count
     * @param entryCount current entry count
     * @param hitCount cache hit count
     * @param missCount cache miss count
     * @param loadCount cache load count
     * @param evictionCount cache eviction count
     * @param invalidationCount cache invalidation count
     */
    @ConstructorProperties({ "pageLimit", "pageCount", "entryCount",
            "hitCount", "missCount", "loadCount", "evictionCount",
            "invalidationCount" })
    @SuppressWarnings("java:S107")
    public ChunkStoreCacheReportResponse(final int pageLimit,
            final int pageCount, final long entryCount, final long hitCount,
            final long missCount, final long loadCount,
            final long evictionCount, final long invalidationCount) {
        this.pageLimit = pageLimit;
        this.pageCount = pageCount;
        this.entryCount = entryCount;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.loadCount = loadCount;
        this.evictionCount = evictionCount;
        this.invalidationCount = invalidationCount;
    }

    /**
     * Returns cache page limit.
     *
     * @return cache page limit
     */
    public int pageLimit() {
        return pageLimit;
    }

    /**
     * Returns current page count.
     *
     * @return current page count
     */
    public int pageCount() {
        return pageCount;
    }

    /**
     * Returns current entry count.
     *
     * @return current entry count
     */
    public long entryCount() {
        return entryCount;
    }

    /**
     * Returns cache hit count.
     *
     * @return cache hit count
     */
    public long hitCount() {
        return hitCount;
    }

    /**
     * Returns cache miss count.
     *
     * @return cache miss count
     */
    public long missCount() {
        return missCount;
    }

    /**
     * Returns cache load count.
     *
     * @return cache load count
     */
    public long loadCount() {
        return loadCount;
    }

    /**
     * Returns cache eviction count.
     *
     * @return cache eviction count
     */
    public long evictionCount() {
        return evictionCount;
    }

    /**
     * Returns cache invalidation count.
     *
     * @return cache invalidation count
     */
    public long invalidationCount() {
        return invalidationCount;
    }
}
