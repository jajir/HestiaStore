package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SegmentRegistryCacheTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void getLoadsOnceAndReturnsSameInstance() {
        final AtomicInteger loads = new AtomicInteger();
        final SegmentRegistryCache<Integer, Object> cache = new SegmentRegistryCache<>(
                10, key -> {
                    loads.incrementAndGet();
                    return new Object();
                }, value -> {
                });

        final Object first = cache.get(1);
        final Object second = cache.get(1);

        assertSame(first, second);
        assertEquals(1, loads.get());
    }

    @Test
    void getBlocksSameKeyWhileLoading() throws Exception {
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                10, key -> {
                    loadStarted.countDown();
                    awaitLatch(allowLoad);
                    return "value";
                }, value -> {
                });

        final Future<String> first = executor.submit(() -> cache.get(1));
        loadStarted.await(1, TimeUnit.SECONDS);
        final Future<String> second = executor.submit(() -> cache.get(1));

        assertFalse(second.isDone());
        allowLoad.countDown();

        assertEquals("value", first.get(1, TimeUnit.SECONDS));
        assertEquals("value", second.get(1, TimeUnit.SECONDS));
    }

    @Test
    void getDifferentKeysDoNotBlock() throws Exception {
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                10, key -> {
                    if (key == 1) {
                        loadStarted.countDown();
                        awaitLatch(allowLoad);
                        return "slow";
                    }
                    return "fast";
                }, value -> {
                });

        final Future<String> slow = executor.submit(() -> cache.get(1));
        loadStarted.await(1, TimeUnit.SECONDS);

        assertEquals("fast", cache.get(2));

        allowLoad.countDown();
        assertEquals("slow", slow.get(1, TimeUnit.SECONDS));
    }

    @Test
    void evictsLeastRecentlyUsedWhenLimitExceeded() {
        final List<Integer> evicted = new CopyOnWriteArrayList<>();
        final SegmentRegistryCache<Integer, Integer> cache = new SegmentRegistryCache<>(
                2, key -> key, evicted::add);

        cache.get(1);
        cache.get(2);
        cache.get(1); // key 2 becomes the least recently used
        cache.get(3);

        assertTrue(cache.getSize() <= 2);
        assertEquals(1, evicted.size());
        assertEquals(2, evicted.get(0));
    }

    @Test
    void waitsForUnloadThenReloadsSameKey() throws Exception {
        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch unloadStarted = new CountDownLatch(1);
        final CountDownLatch allowUnload = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                2, key -> "value-" + loads.incrementAndGet(), value -> {
                    unloadStarted.countDown();
                    awaitLatch(allowUnload);
                });

        assertEquals("value-1", cache.get(1));

        final Future<SegmentRegistryCache.InvalidateStatus> invalidation = executor
                .submit(() -> cache.invalidate(1));
        unloadStarted.await(1, TimeUnit.SECONDS);

        final Future<String> reloaded = executor.submit(() -> cache.get(1));

        assertThrows(TimeoutException.class,
                () -> reloaded.get(100, TimeUnit.MILLISECONDS));

        allowUnload.countDown();

        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                invalidation.get(1, TimeUnit.SECONDS));
        assertEquals("value-2", reloaded.get(1, TimeUnit.SECONDS));
        assertEquals(2, loads.get());
    }

    /**
     * Demonstrates bug: eviction/unload can run while a value is still in use
     * without refCount pinning.
     * 
     * @throws Exception
     */
    @Test
    void invalidateCanUnloadValueWhileInUse() throws Exception {
        final CountDownLatch inUse = new CountDownLatch(1);
        final CountDownLatch allowFinish = new CountDownLatch(1);
        final AtomicReference<TrackedValue> current = new AtomicReference<>();
        final SegmentRegistryCache<Integer, TrackedValue> cache = new SegmentRegistryCache<>(
                2, key -> {
                    final TrackedValue value = new TrackedValue();
                    current.set(value);
                    return value;
                }, TrackedValue::close);

        final Future<Void> user = executor.submit(() -> {
            cache.get(1);
            inUse.countDown();
            awaitLatch(allowFinish);
            return null;
        });
        /**
         * This test prove nothing. I get object, that evict that element and
         * thats all.
         */

        assertTrue(inUse.await(1, TimeUnit.SECONDS),
                "Value was not observed in use");

        cache.invalidate(1);

        assertFalse(current.get().isClosed(),
                "Value should not be closed while still in use");

        allowFinish.countDown();
        user.get(1, TimeUnit.SECONDS);
    }

    private static final class TrackedValue {
        private final AtomicBoolean closed = new AtomicBoolean();

        void close() {
            closed.set(true);
        }

        boolean isClosed() {
            return closed.get();
        }
    }

    private static void awaitLatch(final CountDownLatch latch) {
        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
