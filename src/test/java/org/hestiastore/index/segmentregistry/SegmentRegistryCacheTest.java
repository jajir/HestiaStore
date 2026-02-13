package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 5, unit = TimeUnit.SECONDS)
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
    void getSameKeyUnderContentionLoadsOnlyOnce() throws Exception {
        final int callers = 24;
        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, Object> cache = new SegmentRegistryCache<>(
                10, key -> {
                    loads.incrementAndGet();
                    loadStarted.countDown();
                    awaitLatch(allowLoad);
                    return new Object();
                }, value -> {
                });

        final List<Future<Object>> futures = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            futures.add(executor.submit(() -> {
                awaitLatch(start);
                return cache.get(1);
            }));
        }

        start.countDown();
        assertTrue(loadStarted.await(1, TimeUnit.SECONDS));
        allowLoad.countDown();

        Object first = null;
        for (final Future<Object> future : futures) {
            final Object value = future.get(1, TimeUnit.SECONDS);
            if (first == null) {
                first = value;
            } else {
                assertSame(first, value);
            }
        }
        assertEquals(1, loads.get());
    }

    @Test
    void getLoadFailurePropagatesToAllWaiters() throws Exception {
        final int waiters = 10;
        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch waitersReady = new CountDownLatch(waiters);
        final CountDownLatch startWaiters = new CountDownLatch(1);
        final CountDownLatch allowFailure = new CountDownLatch(1);
        final RuntimeException expected = new RuntimeException("load failed");
        final SegmentRegistryCache<Integer, Object> cache = new SegmentRegistryCache<>(
                10, key -> {
                    loads.incrementAndGet();
                    loadStarted.countDown();
                    awaitLatch(allowFailure);
                    throw expected;
                }, value -> {
                });

        final Future<Object> first = executor.submit(() -> cache.get(1));
        assertTrue(loadStarted.await(1, TimeUnit.SECONDS));

        final List<Future<Object>> waiterFutures = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            waiterFutures.add(executor.submit(() -> {
                waitersReady.countDown();
                awaitLatch(startWaiters);
                return cache.get(1);
            }));
        }
        assertTrue(waitersReady.await(1, TimeUnit.SECONDS));
        startWaiters.countDown();
        Thread.sleep(30L);
        allowFailure.countDown();

        final ExecutionException firstFailure = assertThrows(
                ExecutionException.class,
                () -> first.get(1, TimeUnit.SECONDS));
        assertSame(expected, firstFailure.getCause());
        for (final Future<Object> waiter : waiterFutures) {
            final ExecutionException waiterFailure = assertThrows(
                    ExecutionException.class,
                    () -> waiter.get(1, TimeUnit.SECONDS));
            assertSame(expected, waiterFailure.getCause());
        }
        assertEquals(1, loads.get());
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
    void removeLastRecentUsedSegmentSkipsExceptKey() {
        final List<Integer> evicted = new CopyOnWriteArrayList<>();
        final SegmentRegistryCache<Integer, Integer> cache = new SegmentRegistryCache<>(
                10, key -> key, evicted::add);

        cache.get(1);
        cache.get(2);
        cache.get(3);
        cache.get(2);
        cache.get(3);

        assertTrue(cache.removeLastRecentUsedSegment(1));

        assertEquals(1, evicted.size());
        assertEquals(2, evicted.get(0));
        assertEquals(1, cache.get(1));
    }

    @Test
    void removeLastRecentUsedSegmentSkipsBusyCandidateWithoutStall() {
        final List<Integer> evicted = new CopyOnWriteArrayList<>();
        final SegmentRegistryCache<Integer, Integer> cache = new SegmentRegistryCache<>(
                10, key -> key, evicted::add);

        cache.get(1);
        cache.get(2);
        cache.get(3);
        cache.get(2);
        cache.get(3);
        cache.retain(2);
        try {
            assertTrue(cache.removeLastRecentUsedSegment(1));
        } finally {
            cache.release(2);
        }

        assertEquals(1, evicted.size());
        assertEquals(3, evicted.get(0));
        assertEquals(2, cache.get(2));
    }

    @Test
    void evictionCloseRunsAsyncAndRemovalHappensAfterCloseSuccess()
            throws Exception {
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final CountDownLatch allowClose = new CountDownLatch(1);
        final ExecutorService unloadExecutor = Executors.newSingleThreadExecutor();
        try {
            final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                    1, key -> "value-" + key, value -> {
                        closeStarted.countDown();
                        awaitLatch(allowClose);
                    }, unloadExecutor);

            assertEquals("value-1", cache.get(1));

            final Future<String> loadSecond = executor.submit(() -> cache.get(2));
            assertEquals("value-2", loadSecond.get(300, TimeUnit.MILLISECONDS));
            assertTrue(closeStarted.await(1, TimeUnit.SECONDS));
            assertEquals(2, cache.getSize());

            allowClose.countDown();
            waitUntil(() -> cache.getSize() == 1, 1000);
        } finally {
            unloadExecutor.shutdownNow();
        }
    }

    @Test
    void failedEvictionCloseLeavesEntryUnloadingAndBusy() throws Exception {
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final ExecutorService unloadExecutor = Executors.newSingleThreadExecutor();
        try {
            final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                    1, key -> "value-" + key, value -> {
                        closeStarted.countDown();
                        throw new IllegalStateException("close failed");
                    }, unloadExecutor);

            assertEquals("value-1", cache.get(1));
            assertEquals("value-2", cache.get(2));
            assertTrue(closeStarted.await(1, TimeUnit.SECONDS));

            assertThrows(SegmentRegistryCache.EntryBusyException.class,
                    () -> cache.get(1));
            assertEquals(2, cache.getSize());
        } finally {
            unloadExecutor.shutdownNow();
        }
    }

    @Test
    void getReturnsBusyWhileSameKeyIsUnloadingThenReloadsAfterRemoval()
            throws Exception {
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

        assertThrows(SegmentRegistryCache.EntryBusyException.class,
                () -> cache.get(1));

        allowUnload.countDown();

        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                invalidation.get(1, TimeUnit.SECONDS));
        assertEquals("value-2", cache.get(1));
        assertEquals(2, loads.get());
    }

    @Test
    void unloadingOneKeyDoesNotBlockOtherKeys() throws Exception {
        final CountDownLatch unloadStarted = new CountDownLatch(1);
        final CountDownLatch allowUnload = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                10, key -> "value-" + key, value -> {
                    unloadStarted.countDown();
                    awaitLatch(allowUnload);
                });

        assertEquals("value-1", cache.get(1));

        final Future<SegmentRegistryCache.InvalidateStatus> invalidation = executor
                .submit(() -> cache.invalidate(1));
        assertTrue(unloadStarted.await(1, TimeUnit.SECONDS));

        assertEquals("value-2", cache.get(2));
        assertThrows(SegmentRegistryCache.EntryBusyException.class,
                () -> cache.get(1));

        allowUnload.countDown();
        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                invalidation.get(1, TimeUnit.SECONDS));
    }

    @Test
    void invalidateIsBusyWhileValueIsRetained() {
        final SegmentRegistryCache<Integer, TrackedValue> cache = new SegmentRegistryCache<>(
                2, key -> new TrackedValue(), TrackedValue::close);

        final TrackedValue value = cache.get(1);
        cache.retain(1);
        try {
            assertEquals(SegmentRegistryCache.InvalidateStatus.BUSY,
                    cache.invalidate(1));
            assertFalse(value.isClosed(),
                    "Value should not be closed while retained");
        } finally {
            cache.release(1);
        }

        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                cache.invalidate(1));
        assertTrue(value.isClosed(), "Value should be closed after release");
    }

    @Test
    void entryTransitionsFromLoadingToReadyToUnloading() {
        final SegmentRegistryCache.Entry<String> entry = new SegmentRegistryCache.Entry<>(
                7L);

        assertTrue(entry.tryStartLoad());

        entry.finishLoad("value");
        assertEquals("value", entry.waitWhileLoading(8L));

        assertEquals("value", entry.tryStartUnload());
        entry.finishUnload();

        assertThrows(SegmentRegistryCache.EntryBusyException.class,
                () -> entry.waitWhileLoading(9L));
    }

    @Test
    void entryInvalidTransitionsFailPredictably() {
        final SegmentRegistryCache.Entry<String> entry = new SegmentRegistryCache.Entry<>(
                1L);

        entry.finishLoad("value");

        assertThrows(IllegalStateException.class,
                () -> entry.finishLoad("other"));
        assertThrows(IllegalStateException.class,
                () -> entry.fail(new IllegalStateException("boom")));
        assertThrows(IllegalStateException.class, entry::finishUnload);
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

    private static void waitUntil(final java.util.function.BooleanSupplier done,
            final long timeoutMillis) throws TimeoutException {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!done.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new TimeoutException("Condition not met in time");
            }
            try {
                Thread.sleep(10L);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new TimeoutException("Interrupted while waiting");
            }
        }
    }
}
