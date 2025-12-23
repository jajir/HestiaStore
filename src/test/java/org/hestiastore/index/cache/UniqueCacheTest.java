package org.hestiastore.index.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UniqueCacheTest {

    private UniqueCache<Integer, String> cache;

    @BeforeEach
    void setup() {
        cache = new UniqueCache<>((i1, i2) -> i1 - i2, 100);
    }

    @AfterEach
    void tearDown() {
        cache = null;
    }

    @Test
    void test_basic_function() {
        cache.put(Entry.of(10, "hello"));
        cache.put(Entry.of(13, "my"));
        cache.put(Entry.of(15, "dear"));

        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(3, cache.size());
        assertEquals(Entry.of(10, "hello"), out.get(0));
        assertEquals(Entry.of(13, "my"), out.get(1));
        assertEquals(Entry.of(15, "dear"), out.get(2));
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    void test_basic_function_different_order() {
        cache.put(Entry.of(15, "dear"));
        cache.put(Entry.of(13, "my"));
        cache.put(Entry.of(-199, "hello"));

        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(3, cache.size());
        assertEquals(Entry.of(-199, "hello"), out.get(0));
        assertEquals(Entry.of(13, "my"), out.get(1));
        assertEquals(Entry.of(15, "dear"), out.get(2));
        cache.clear();
        assertEquals(0, cache.size());
    }

    /**
     * Test verify that stream is not sorted.
     * 
     * @
     */
    @Test
    void test_getAsSortedList_sorting() {
        cache.put(Entry.of(15, "dear"));
        cache.put(Entry.of(13, "my"));
        cache.put(Entry.of(-199, "hello"));
        cache.put(Entry.of(-19, "Duck"));

        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(4, cache.size());
        assertEquals(Entry.of(-199, "hello"), out.get(0));
        assertEquals(Entry.of(-19, "Duck"), out.get(1));
        assertEquals(Entry.of(13, "my"), out.get(2));
        assertEquals(Entry.of(15, "dear"), out.get(3));
        assertEquals(4, cache.size());
    }

    @Test
    void test_last_value_wins_for_same_key() {
        cache.put(Entry.of(10, "hello"));
        cache.put(Entry.of(10, "my"));
        cache.put(Entry.of(10, "dear"));

        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(1, cache.size());
        assertEquals(Entry.of(10, "dear"), out.get(0));
    }

    @Test
    void test_constructor_null_comparator_throws() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new UniqueCache<Integer, String>(null, 100));
        assertEquals("Property 'keyComparator' must not be null.",
                e.getMessage());
    }

    @Test
    void test_get_with_null_key_throws() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> cache.get(null));
        assertEquals("Property 'key' must not be null.", e.getMessage());
    }

    @Test
    void test_get_non_existing_returns_null() {
        cache.put(Entry.of(1, "a"));
        cache.put(Entry.of(2, "b"));
        assertNull(cache.get(100));
    }

    @Test
    void test_isEmpty_and_size_consistency() {
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());

        cache.put(Entry.of(7, "x"));
        assertFalse(cache.isEmpty());
        assertEquals(1, cache.size());

        cache.clear();
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
    }

    @Test
    void test_snapshotAndClear_returnsSnapshotAndEmptiesCache() {
        cache.put(Entry.of(1, "a"));
        cache.put(Entry.of(2, "b"));
        cache.put(Entry.of(3, "c"));

        final List<Entry<Integer, String>> snapshot = cache.snapshotAndClear();

        assertEquals(3, snapshot.size());
        assertTrue(snapshot.contains(Entry.of(1, "a")));
        assertTrue(snapshot.contains(Entry.of(2, "b")));
        assertTrue(snapshot.contains(Entry.of(3, "c")));
        assertTrue(cache.isEmpty());
    }

    @Test
    void test_threadSafe_cache_handles_concurrent_updates() throws Exception {
        final UniqueCache<Integer, String> threadSafe = new UniqueCacheSynchronizenizedAdapter<>(
                new UniqueCache<>(Integer::compareTo, 16));
        final int threads = 6;
        final int perThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int t = 0; t < threads; t++) {
                final int workerId = t;
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        final int base = workerId * perThread;
                        for (int i = 0; i < perThread; i++) {
                            threadSafe.put(
                                    Entry.of(base + i, "v" + i));
                        }
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS),
                    "Workers did not start in time");
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS),
                    "Workers did not finish in time");
        } finally {
            executor.shutdownNow();
        }

        assertEquals(threads * perThread, threadSafe.size());
    }

}
