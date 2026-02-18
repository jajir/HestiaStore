package org.hestiastore.index.segmentregistry;

/**
 * Immutable snapshot of segment registry cache counters.
 *
 * @param hitCount      successful cache hits
 * @param missCount     cache misses that required loading
 * @param loadCount     successful loads
 * @param evictionCount cache evictions due to size pressure
 * @param size          current number of cached entries
 * @param limit         configured cache size limit
 */
public record SegmentRegistryCacheStats(long hitCount, long missCount,
        long loadCount, long evictionCount, int size, int limit) {

    /**
     * Creates validated cache stats snapshot.
     *
     * @param hitCount      successful cache hits
     * @param missCount     cache misses that required loading
     * @param loadCount     successful loads
     * @param evictionCount cache evictions due to size pressure
     * @param size          current number of cached entries
     * @param limit         configured cache size limit
     */
    public SegmentRegistryCacheStats {
        if (hitCount < 0L || missCount < 0L || loadCount < 0L
                || evictionCount < 0L) {
            throw new IllegalArgumentException("counts must be >= 0");
        }
        if (size < 0 || limit < 0) {
            throw new IllegalArgumentException("size/limit must be >= 0");
        }
    }

    /**
     * Empty cache stats snapshot.
     *
     * @return zeroed snapshot
     */
    public static SegmentRegistryCacheStats empty() {
        return new SegmentRegistryCacheStats(0L, 0L, 0L, 0L, 0, 0);
    }
}
