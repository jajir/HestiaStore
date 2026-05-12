package org.hestiastore.index.chunkstorecache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;

class LruChunkStoreCacheTest {

    private static final Comparator<Integer> COMPARATOR =
            Integer::compareTo;

    @Test
    void disabledModeBypassesStorageAndMetrics() {
        final LruChunkStoreCache<Integer, String> cache =
                new LruChunkStoreCache<>(0);
        final AtomicInteger loads = new AtomicInteger();

        assertEquals("one", cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "one")));
        assertEquals("one", cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "one")));

        final ChunkStoreCacheStats stats = cache.stats();
        assertEquals(2, loads.get());
        assertEquals(0, stats.pageLimit());
        assertEquals(0, stats.pageCount());
        assertEquals(0L, stats.entryCount());
        assertEquals(0L, stats.hitCount());
        assertEquals(0L, stats.missCount());
        assertEquals(0L, stats.loadCount());
    }

    @Test
    void missLoadsOnceAndFollowingReadHits() {
        final LruChunkStoreCache<Integer, String> cache =
                new LruChunkStoreCache<>(2);
        final AtomicInteger loads = new AtomicInteger();

        assertEquals("one", cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "one")));
        assertEquals("one", cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "unexpected")));

        final ChunkStoreCacheStats stats = cache.stats();
        assertEquals(1, loads.get());
        assertEquals(1, stats.pageCount());
        assertEquals(1L, stats.entryCount());
        assertEquals(1L, stats.hitCount());
        assertEquals(1L, stats.missCount());
        assertEquals(1L, stats.loadCount());
    }

    @Test
    void lruEvictionRespectsPageLimit() {
        final LruChunkStoreCache<Integer, String> cache =
                new LruChunkStoreCache<>(2);
        final AtomicInteger loads = new AtomicInteger();

        cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "one"));
        cache.find("segment-1", 1L, 20L, 2, COMPARATOR,
                () -> loadedPage(loads, 2, "two"));
        cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "unexpected"));
        cache.find("segment-1", 1L, 30L, 3, COMPARATOR,
                () -> loadedPage(loads, 3, "three"));

        final ChunkStoreCacheStats stats = cache.stats();
        assertEquals(3, loads.get());
        assertEquals(2, stats.pageCount());
        assertEquals(1L, stats.evictionCount());
        assertEquals(1L, stats.hitCount());
    }

    @Test
    void updateLimitZeroClearsAndDisablesCache() {
        final LruChunkStoreCache<Integer, String> cache =
                new LruChunkStoreCache<>(1);
        final AtomicInteger loads = new AtomicInteger();

        cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "one"));
        cache.updateLimit(0);
        cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "one"));

        final ChunkStoreCacheStats stats = cache.stats();
        assertEquals(2, loads.get());
        assertEquals(0, stats.pageLimit());
        assertEquals(0, stats.pageCount());
        assertEquals(1L, stats.loadCount());
        assertEquals(1L, stats.invalidationCount());
    }

    @Test
    void activeVersionIsPartOfCacheKey() {
        final LruChunkStoreCache<Integer, String> cache =
                new LruChunkStoreCache<>(4);
        final AtomicInteger loads = new AtomicInteger();

        assertEquals("old", cache.find("segment-1", 1L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "old")));
        assertEquals("new", cache.find("segment-1", 2L, 10L, 1, COMPARATOR,
                () -> loadedPage(loads, 1, "new")));

        final ChunkStoreCacheStats stats = cache.stats();
        assertEquals(2, loads.get());
        assertEquals(2, stats.pageCount());
        assertEquals(2L, stats.loadCount());
    }

    private static ParsedChunkPage<Integer, String> loadedPage(
            final AtomicInteger loads, final int key, final String value) {
        loads.incrementAndGet();
        return ParsedChunkPage.of(List.of(Entry.of(key, value)));
    }
}
