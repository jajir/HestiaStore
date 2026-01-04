package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class SegmentCacheAsyncAdapterTest {

    private final TypeDescriptorInteger keyType = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueType = new TypeDescriptorShortString();
    private static final int DEFAULT_MAX_BUFFERED = 1000;
    private static final int DEFAULT_MAX_DURING_FLUSH = 1000;
    private static final int DEFAULT_MAX_SEGMENT_CACHE = 1024;

    @Test
    void adapter_delegates_to_segment_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A")), DEFAULT_MAX_BUFFERED,
                DEFAULT_MAX_DURING_FLUSH, DEFAULT_MAX_SEGMENT_CACHE);
        final SegmentCacheAsyncAdapter<Integer, String> adapter = new SegmentCacheAsyncAdapter<>(
                cache);

        adapter.putToWriteCache(Entry.of(2, "B"));

        assertEquals("A", adapter.get(1));
        assertEquals(2, adapter.size());
        assertEquals(2, adapter.sizeWithoutTombstones());
        assertEquals(List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                adapter.getAsSortedList());
    }

    @Test
    void adapter_supports_concurrent_writes() throws Exception {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_FLUSH,
                DEFAULT_MAX_SEGMENT_CACHE);
        final SegmentCacheAsyncAdapter<Integer, String> adapter = new SegmentCacheAsyncAdapter<>(
                cache);

        final int threads = 4;
        final int perThread = 25;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threads);
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int offset = t * perThread;
                executor.execute(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < perThread; i++) {
                            adapter.putToWriteCache(
                                    Entry.of(offset + i, "v" + (offset + i)));
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            startLatch.countDown();
            final boolean completed = doneLatch.await(5, TimeUnit.SECONDS);
            assertTrue(completed);
        } finally {
            executor.shutdownNow();
        }

        assertEquals(threads * perThread, adapter.size());
    }

    @Test
    void adapter_exposes_write_cache_snapshot_and_clear() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_FLUSH,
                DEFAULT_MAX_SEGMENT_CACHE);
        final SegmentCacheAsyncAdapter<Integer, String> adapter = new SegmentCacheAsyncAdapter<>(
                cache);

        adapter.putToWriteCache(Entry.of(2, "B"));
        adapter.putToWriteCache(Entry.of(1, "A"));

        assertEquals(List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                adapter.getWriteCacheAsSortedList());

        adapter.clearWriteCache();
        assertEquals(0, adapter.size());
    }

    @Test
    void adapter_merges_write_cache_into_delta_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_FLUSH,
                DEFAULT_MAX_SEGMENT_CACHE);
        final SegmentCacheAsyncAdapter<Integer, String> adapter = new SegmentCacheAsyncAdapter<>(
                cache);

        adapter.putToWriteCache(Entry.of(1, "A"));
        adapter.putToWriteCache(Entry.of(2, "B"));

        adapter.mergeWriteCacheToDeltaCache();

        assertEquals(List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                adapter.getAsSortedList());
        assertTrue(adapter.getWriteCacheAsSortedList().isEmpty());
    }
}
