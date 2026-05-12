package org.hestiastore.index.chunkstorecache;

import org.hestiastore.index.Vldtn;

/**
 * Immutable parsed chunk page cache statistics.
 */
public final class ChunkStoreCacheStats {

    private final int pageLimit;
    private final int pageCount;
    private final long entryCount;
    private final long hitCount;
    private final long missCount;
    private final long loadCount;
    private final long evictionCount;
    private final long invalidationCount;

    @SuppressWarnings("java:S107")
    public ChunkStoreCacheStats(final int pageLimit, final int pageCount,
            final long entryCount, final long hitCount, final long missCount,
            final long loadCount, final long evictionCount,
            final long invalidationCount) {
        this.pageLimit = Vldtn.requireGreaterThanOrEqualToZero(pageLimit,
                "pageLimit");
        this.pageCount = Vldtn.requireGreaterThanOrEqualToZero(pageCount,
                "pageCount");
        this.entryCount = Vldtn.requireGreaterThanOrEqualToZero(entryCount,
                "entryCount");
        this.hitCount = Vldtn.requireGreaterThanOrEqualToZero(hitCount,
                "hitCount");
        this.missCount = Vldtn.requireGreaterThanOrEqualToZero(missCount,
                "missCount");
        this.loadCount = Vldtn.requireGreaterThanOrEqualToZero(loadCount,
                "loadCount");
        this.evictionCount = Vldtn.requireGreaterThanOrEqualToZero(
                evictionCount, "evictionCount");
        this.invalidationCount = Vldtn.requireGreaterThanOrEqualToZero(
                invalidationCount, "invalidationCount");
    }

    public int pageLimit() {
        return pageLimit;
    }

    public int pageCount() {
        return pageCount;
    }

    public long entryCount() {
        return entryCount;
    }

    public long hitCount() {
        return hitCount;
    }

    public long missCount() {
        return missCount;
    }

    public long loadCount() {
        return loadCount;
    }

    public long evictionCount() {
        return evictionCount;
    }

    public long invalidationCount() {
        return invalidationCount;
    }
}
