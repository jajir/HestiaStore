package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Registry cache metrics section inside an index report payload.
 */
@SuppressWarnings("java:S6206")
public final class RegistryCacheReportResponse {

    private final long hitCount;
    private final long missCount;
    private final long loadCount;
    private final long evictionCount;
    private final int size;
    private final int limit;

    /**
     * Creates registry cache metrics.
     *
     * @param hitCount cache hit count
     * @param missCount cache miss count
     * @param loadCount cache load count
     * @param evictionCount cache eviction count
     * @param size current cache size
     * @param limit cache size limit
     */
    @ConstructorProperties({ "hitCount", "missCount", "loadCount",
            "evictionCount", "size", "limit" })
    public RegistryCacheReportResponse(final long hitCount,
            final long missCount, final long loadCount,
            final long evictionCount, final int size, final int limit) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.loadCount = loadCount;
        this.evictionCount = evictionCount;
        this.size = size;
        this.limit = limit;
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
     * Returns current cache size.
     *
     * @return current cache size
     */
    public int size() {
        return size;
    }

    /**
     * Returns cache size limit.
     *
     * @return cache size limit
     */
    public int limit() {
        return limit;
    }
}
