package org.hestiastore.index.segmentindex.runtimemonitoring.model;

/**
 * User-facing parsed chunk page cache metrics.
 */
public final class SegmentIndexChunkStoreCacheMetrics {

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
     * @param pageLimit configured page limit
     * @param pageCount current page count
     * @param entryCount current entry count
     * @param hitCount cache hit count
     * @param missCount cache miss count
     * @param loadCount cache load count
     * @param evictionCount cache eviction count
     * @param invalidationCount cache invalidation count
     */
    @SuppressWarnings("java:S107")
    public SegmentIndexChunkStoreCacheMetrics(final int pageLimit,
            final int pageCount, final long entryCount, final long hitCount,
            final long missCount, final long loadCount,
            final long evictionCount, final long invalidationCount) {
        this.pageLimit = MetricModelValidation.nonNegative(pageLimit,
                "pageLimit");
        this.pageCount = MetricModelValidation.nonNegative(pageCount,
                "pageCount");
        this.entryCount = MetricModelValidation.nonNegative(entryCount,
                "entryCount");
        this.hitCount = MetricModelValidation.nonNegative(hitCount,
                "hitCount");
        this.missCount = MetricModelValidation.nonNegative(missCount,
                "missCount");
        this.loadCount = MetricModelValidation.nonNegative(loadCount,
                "loadCount");
        this.evictionCount = MetricModelValidation.nonNegative(evictionCount,
                "evictionCount");
        this.invalidationCount = MetricModelValidation.nonNegative(
                invalidationCount, "invalidationCount");
    }

    /**
     * Returns configured page limit.
     *
     * @return page limit
     */
    public int pageLimit() {
        return pageLimit;
    }

    /**
     * Returns current page count.
     *
     * @return page count
     */
    public int pageCount() {
        return pageCount;
    }

    /**
     * Returns current entry count.
     *
     * @return entry count
     */
    public long entryCount() {
        return entryCount;
    }

    /**
     * Returns cache hit count.
     *
     * @return hit count
     */
    public long hitCount() {
        return hitCount;
    }

    /**
     * Returns cache miss count.
     *
     * @return miss count
     */
    public long missCount() {
        return missCount;
    }

    /**
     * Returns cache load count.
     *
     * @return load count
     */
    public long loadCount() {
        return loadCount;
    }

    /**
     * Returns cache eviction count.
     *
     * @return eviction count
     */
    public long evictionCount() {
        return evictionCount;
    }

    /**
     * Returns cache invalidation count.
     *
     * @return invalidation count
     */
    public long invalidationCount() {
        return invalidationCount;
    }
}
