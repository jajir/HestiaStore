package org.hestiastore.index.segmentindex.runtimemonitoring.model;

/**
 * User-facing segment registry cache metrics.
 */
public final class SegmentIndexRegistryCacheMetrics {

    private final long hitCount;
    private final long missCount;
    private final long loadCount;
    private final long evictionCount;
    private final int size;
    private final int limit;

    /**
     * Creates segment registry cache metrics.
     *
     * @param hitCount cache hit count
     * @param missCount cache miss count
     * @param loadCount cache load count
     * @param evictionCount cache eviction count
     * @param size current cache size
     * @param limit configured cache limit
     */
    public SegmentIndexRegistryCacheMetrics(final long hitCount,
            final long missCount, final long loadCount,
            final long evictionCount, final int size, final int limit) {
        this.hitCount = MetricModelValidation.nonNegative(hitCount,
                "hitCount");
        this.missCount = MetricModelValidation.nonNegative(missCount,
                "missCount");
        this.loadCount = MetricModelValidation.nonNegative(loadCount,
                "loadCount");
        this.evictionCount = MetricModelValidation.nonNegative(evictionCount,
                "evictionCount");
        this.size = MetricModelValidation.nonNegative(size, "size");
        this.limit = MetricModelValidation.nonNegative(limit, "limit");
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
     * Returns current cache size.
     *
     * @return cache size
     */
    public int size() {
        return size;
    }

    /**
     * Returns configured cache limit.
     *
     * @return cache limit
     */
    public int limit() {
        return limit;
    }
}
