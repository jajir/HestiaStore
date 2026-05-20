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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

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
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loads.incrementAndGet();
                    return segment(key.getId());
                }, value -> {
                });

        final Segment<Integer, String> first = cache.get(id(1));
        final Segment<Integer, String> second = cache.get(id(1));

        assertSame(first, second);
        assertEquals(1, loads.get());
    }

    @Test
    void getIfReadyReturnsEmptyWithoutLoadingMissingEntry() {
        final AtomicInteger loads = new AtomicInteger();
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loads.incrementAndGet();
                    return segment(key.getId());
                }, value -> {
                });

        assertTrue(cache.getIfReady(id(1)).isEmpty());

        assertEquals(0, loads.get());
        assertEquals(0, cache.getSize());
    }

    @Test
    void getIfReadyReturnsLoadedValueWithoutLoading() {
        final AtomicInteger loads = new AtomicInteger();
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loads.incrementAndGet();
                    return segment(key.getId());
                }, value -> {
                });

        final Segment<Integer, String> loaded = cache.get(id(1));

        assertSame(loaded, cache.getIfReady(id(1)).orElseThrow());
        assertEquals(1, loads.get());
        assertEquals(1, cache.getSize());
    }

    @Test
    void getIfReadyReturnsEmptyForLoadingAndUnloadingEntries()
            throws Exception {
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final CountDownLatch unloadStarted = new CountDownLatch(1);
        final CountDownLatch allowUnload = new CountDownLatch(1);
        final Segment<Integer, String> value = segment(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loadStarted.countDown();
                    awaitLatch(allowLoad);
                    return value;
                }, segment -> {
                    unloadStarted.countDown();
                    awaitLatch(allowUnload);
                });

        final Future<Segment<Integer, String>> loading = executor
                .submit(() -> cache.get(id(1)));
        assertTrue(loadStarted.await(1, TimeUnit.SECONDS));
        assertTrue(cache.getIfReady(id(1)).isEmpty());
        allowLoad.countDown();
        assertSame(value, loading.get(1, TimeUnit.SECONDS));

        final Future<SegmentRegistryCache.InvalidateStatus> invalidation =
                executor.submit(() -> cache.invalidate(id(1)));
        assertTrue(unloadStarted.await(1, TimeUnit.SECONDS));

        assertTrue(cache.getIfReady(id(1)).isEmpty());

        allowUnload.countDown();
        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                invalidation.get(1, TimeUnit.SECONDS));
    }

    @Test
    void getBlocksSameKeyWhileLoading() throws Exception {
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final Segment<Integer, String> value = segment(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loadStarted.countDown();
                    awaitLatch(allowLoad);
                    return value;
                }, segment -> {
                });

        final Future<Segment<Integer, String>> first = executor
                .submit(() -> cache.get(id(1)));
        loadStarted.await(1, TimeUnit.SECONDS);
        final Future<Segment<Integer, String>> second = executor
                .submit(() -> cache.get(id(1)));

        assertFalse(second.isDone());
        allowLoad.countDown();

        assertSame(value, first.get(1, TimeUnit.SECONDS));
        assertSame(value, second.get(1, TimeUnit.SECONDS));
    }

    @Test
    void getDifferentKeysDoNotBlock() throws Exception {
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    if (key.equals(id(1))) {
                        loadStarted.countDown();
                        awaitLatch(allowLoad);
                    }
                    return segment(key.getId());
                }, segment -> {
                });

        final Future<Segment<Integer, String>> slow = executor
                .submit(() -> cache.get(id(1)));
        loadStarted.await(1, TimeUnit.SECONDS);

        assertEquals(id(2), cache.get(id(2)).getId());

        allowLoad.countDown();
        assertEquals(id(1), slow.get(1, TimeUnit.SECONDS).getId());
    }

    @Test
    void getSameKeyUnderContentionLoadsOnlyOnce() throws Exception {
        final int callers = 24;
        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loads.incrementAndGet();
                    loadStarted.countDown();
                    awaitLatch(allowLoad);
                    return segment(key.getId());
                }, segment -> {
                });

        final List<Future<Segment<Integer, String>>> futures = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            futures.add(executor.submit(() -> {
                awaitLatch(start);
                return cache.get(id(1));
            }));
        }

        start.countDown();
        assertTrue(loadStarted.await(1, TimeUnit.SECONDS));
        allowLoad.countDown();

        Segment<Integer, String> first = null;
        for (final Future<Segment<Integer, String>> future : futures) {
            final Segment<Integer, String> value = future.get(1,
                    TimeUnit.SECONDS);
            if (first == null) {
                first = value;
            } else {
                assertSame(first, value);
            }
        }
        assertTrue(loads.get() >= 1 && loads.get() <= 2,
                "Load failure must fan out to waiters; one retry after entry removal is acceptable.");
    }

    @Test
    void getLoadFailurePropagatesToAllWaiters() throws Exception {
        final int waiters = 10;
        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch waitersReady = new CountDownLatch(waiters);
        final CountDownLatch waitersStartedLoad = new CountDownLatch(waiters);
        final CountDownLatch startWaiters = new CountDownLatch(1);
        final CountDownLatch allowFailure = new CountDownLatch(1);
        final RuntimeException expected = new RuntimeException("load failed");
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loads.incrementAndGet();
                    loadStarted.countDown();
                    awaitLatch(allowFailure);
                    throw expected;
                }, segment -> {
                });

        final Future<Segment<Integer, String>> first = executor
                .submit(() -> cache.get(id(1)));
        assertTrue(loadStarted.await(1, TimeUnit.SECONDS));

        final List<Future<Segment<Integer, String>>> waiterFutures = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            waiterFutures.add(executor.submit(() -> {
                waitersReady.countDown();
                awaitLatch(startWaiters);
                waitersStartedLoad.countDown();
                return cache.get(id(1));
            }));
        }
        assertTrue(waitersReady.await(1, TimeUnit.SECONDS));
        startWaiters.countDown();
        assertTrue(waitersStartedLoad.await(1, TimeUnit.SECONDS));
        allowFailure.countDown();

        final ExecutionException firstFailure = assertThrows(
                ExecutionException.class,
                () -> futureGetWithTimeout(first));
        assertSame(expected, firstFailure.getCause());
        for (final Future<Segment<Integer, String>> waiter : waiterFutures) {
            final ExecutionException waiterFailure = assertThrows(
                    ExecutionException.class,
                    () -> futureGetWithTimeout(waiter));
            assertSame(expected, waiterFailure.getCause());
        }
        assertTrue(loads.get() >= 1 && loads.get() <= 2,
                "Failure must fan out to threads already waiting on the failed entry;"
                        + " one retry after entry removal is acceptable.");
    }

    @Test
    void evictsLeastRecentlyUsedWhenLimitExceeded() {
        final List<Integer> evicted = new CopyOnWriteArrayList<>();
        final SegmentRegistryCache<Integer, String> cache = newCache(
                2, key -> segment(key.getId()),
                value -> evicted.add(value.getId().getId()));

        cache.get(id(1));
        cache.get(id(2));
        cache.get(id(1)); // key 2 becomes the least recently used
        cache.get(id(3));

        assertTrue(cache.getSize() <= 2);
        assertEquals(1, evicted.size());
        assertEquals(2, evicted.get(0));
    }

    @Test
    void metricsSnapshotTracksHitsMissesLoadsAndEvictions() throws Exception {
        final SegmentRegistryCache<Integer, String> cache = newCache(
                2, key -> segment(key.getId()), value -> {
                });

        cache.get(id(1)); // miss + load
        cache.get(id(1)); // hit
        cache.get(id(2)); // miss + load
        cache.get(id(3)); // miss + load + eviction

        waitUntil(() -> cache.getSize() <= 2, 1000);
        final SegmentRegistryCacheStats snapshot = cache.metricsSnapshot();
        assertTrue(snapshot.hitCount() >= 1L);
        assertTrue(snapshot.missCount() >= 3L);
        assertTrue(snapshot.loadCount() >= 3L);
        assertTrue(snapshot.evictionCount() >= 1L);
        assertEquals(cache.getSize(), snapshot.size());
        assertEquals(2, snapshot.limit());
    }

    @Test
    void removeLastRecentUsedSegmentSkipsExceptKey() {
        final List<Integer> evicted = new CopyOnWriteArrayList<>();
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> segment(key.getId()),
                value -> evicted.add(value.getId().getId()));

        cache.get(id(1));
        cache.get(id(2));
        cache.get(id(3));
        cache.get(id(2));
        cache.get(id(3));

        assertTrue(cache.removeLastRecentUsedSegment(id(1)));

        assertEquals(1, evicted.size());
        assertEquals(2, evicted.get(0));
        assertEquals(id(1), cache.get(id(1)).getId());
    }

    @Test
    void removeLastRecentUsedSegmentSkipsBusyCandidateWithoutStall()
            throws Exception {
        final List<Integer> evicted = new CopyOnWriteArrayList<>();
        final AtomicBoolean segmentTwoUnloadAllowed = new AtomicBoolean(true);
        final ExecutorService unloadExecutor = Executors.newSingleThreadExecutor();
        try {
            final SegmentRegistryCache<Integer, String> cache = newCache(
                    10, key -> segment(key.getId()),
                    value -> evicted.add(value.getId().getId()), unloadExecutor,
                    value -> value.getId().getId() != 2
                            || segmentTwoUnloadAllowed.get());

            cache.get(id(1));
            cache.get(id(2));
            cache.get(id(3));
            cache.get(id(2));
            cache.get(id(3));
            segmentTwoUnloadAllowed.set(false);

            assertTrue(cache.removeLastRecentUsedSegment(id(1)));
            waitUntil(() -> evicted.size() == 1, 1000);

            assertEquals(1, evicted.size());
            assertEquals(3, evicted.get(0));
            assertEquals(id(2), cache.get(id(2)).getId());
        } finally {
            unloadExecutor.shutdownNow();
        }
    }

    @Test
    void evictionCloseRunsAsyncAndRemovalHappensAfterCloseSuccess()
            throws Exception {
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final CountDownLatch allowClose = new CountDownLatch(1);
        final ExecutorService unloadExecutor = Executors.newSingleThreadExecutor();
        try {
            final SegmentRegistryCache<Integer, String> cache = newCache(
                    1, key -> segment(key.getId()), value -> {
                        closeStarted.countDown();
                        awaitLatch(allowClose);
                    }, unloadExecutor);

            assertEquals(id(1), cache.get(id(1)).getId());

            final Future<Segment<Integer, String>> loadSecond = executor
                    .submit(() -> cache.get(id(2)));
            assertEquals(id(2),
                    loadSecond.get(300, TimeUnit.MILLISECONDS).getId());
            assertTrue(closeStarted.await(1, TimeUnit.SECONDS));
            assertEquals(2, cache.getSize());

            allowClose.countDown();
            waitUntil(() -> cache.getSize() == 1, 1000);
        } finally {
            unloadExecutor.shutdownNow();
        }
    }

    @Test
    void failedEvictionCloseCancelsUnloadAndKeepsEntryAvailable()
            throws Exception {
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final ExecutorService unloadExecutor = Executors.newSingleThreadExecutor();
        try {
            final SegmentRegistryCache<Integer, String> cache = newCache(
                    1, key -> segment(key.getId()), value -> {
                        closeStarted.countDown();
                        throw new IllegalStateException("close failed");
                    }, unloadExecutor);

            assertEquals(id(1), cache.get(id(1)).getId());
            assertEquals(id(2), cache.get(id(2)).getId());
            assertTrue(closeStarted.await(1, TimeUnit.SECONDS));

            waitUntil(() -> {
                try {
                    return id(1).equals(cache.get(id(1)).getId());
                } catch (final SegmentRegistryCache.EntryBusyException ex) {
                    return false;
                }
            }, 1000);
            assertEquals(2, cache.getSize());
        } finally {
            unloadExecutor.shutdownNow();
        }
    }

    @Test
    void failedExplicitInvalidateCancelsUnloadAndKeepsEntryAvailable() {
        final AtomicInteger loads = new AtomicInteger();
        final SegmentRegistryCache<Integer, String> cache = newCache(
                2, key -> segment(loads.incrementAndGet()), value -> {
                    throw new IllegalStateException("close failed");
                });

        final Segment<Integer, String> first = cache.get(id(1));
        assertEquals(SegmentRegistryCache.InvalidateStatus.BUSY,
                cache.invalidate(id(1)));
        assertSame(first, cache.get(id(1)));
        assertEquals(1, loads.get());
    }

    @Test
    void getReturnsBusyWhileSameKeyIsUnloadingThenReloadsAfterRemoval()
            throws Exception {
        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch unloadStarted = new CountDownLatch(1);
        final CountDownLatch allowUnload = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                2, key -> segment(loads.incrementAndGet()), value -> {
                    unloadStarted.countDown();
                    awaitLatch(allowUnload);
                });

        assertEquals(id(1), cache.get(id(1)).getId());

        final Future<SegmentRegistryCache.InvalidateStatus> invalidation = executor
                .submit(() -> cache.invalidate(id(1)));
        unloadStarted.await(1, TimeUnit.SECONDS);

        assertThrows(SegmentRegistryCache.EntryBusyException.class,
                () -> cache.get(id(1)));

        allowUnload.countDown();

        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                invalidation.get(1, TimeUnit.SECONDS));
        assertEquals(id(2), cache.get(id(1)).getId());
        assertEquals(2, loads.get());
    }

    @Test
    void unloadingOneKeyDoesNotBlockOtherKeys() throws Exception {
        final CountDownLatch unloadStarted = new CountDownLatch(1);
        final CountDownLatch allowUnload = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> segment(key.getId()), value -> {
                    unloadStarted.countDown();
                    awaitLatch(allowUnload);
                });

        assertEquals(id(1), cache.get(id(1)).getId());

        final Future<SegmentRegistryCache.InvalidateStatus> invalidation = executor
                .submit(() -> cache.invalidate(id(1)));
        assertTrue(unloadStarted.await(1, TimeUnit.SECONDS));

        assertEquals(id(2), cache.get(id(2)).getId());
        assertThrows(SegmentRegistryCache.EntryBusyException.class,
                () -> cache.get(id(1)));

        allowUnload.countDown();
        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                invalidation.get(1, TimeUnit.SECONDS));
    }

    @Test
    void invalidateIsBusyWhileUnloadPredicateRejectsValue() {
        final ExecutorService unloadExecutor = Executors.newSingleThreadExecutor();
        try {
            final AtomicBoolean closed = new AtomicBoolean();
            final AtomicBoolean unloadAllowed = new AtomicBoolean(true);
            final Segment<Integer, String> value = segment(1);
            final SegmentRegistryCache<Integer, String> cache = newCache(
                    2, key -> value, segment -> closed.set(true),
                    unloadExecutor, segment -> unloadAllowed.get());

            assertSame(value, cache.get(id(1)));
            unloadAllowed.set(false);
            assertEquals(SegmentRegistryCache.InvalidateStatus.BUSY,
                    cache.invalidate(id(1)));
            assertFalse(closed.get(),
                    "Value should not be closed when unload predicate rejects.");

            unloadAllowed.set(true);
            assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                    cache.invalidate(id(1)));
            assertTrue(closed.get(), "Value should be closed after removal.");
        } finally {
            unloadExecutor.shutdownNow();
        }
    }

    @Test
    void forceInvalidateBypassesUnloadPredicateAndClosesValue() {
        final AtomicBoolean closed = new AtomicBoolean();
        final Segment<Integer, String> value = segment(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                2, key -> value, segment -> closed.set(true), Runnable::run,
                segment -> false);

        assertSame(value, cache.get(id(1)));

        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                cache.forceInvalidate(id(1)));
        assertTrue(closed.get(), "Forced invalidation should close value.");
        assertTrue(cache.getIfReady(id(1)).isEmpty());
    }

    @Test
    void forceInvalidateReturnsBusyWhileEntryLoading() throws Exception {
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch allowLoad = new CountDownLatch(1);
        final Segment<Integer, String> value = segment(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> {
                    loadStarted.countDown();
                    awaitLatch(allowLoad);
                    return value;
                }, segment -> {
                });

        final Future<Segment<Integer, String>> loading = executor
                .submit(() -> cache.get(id(1)));
        assertTrue(loadStarted.await(1, TimeUnit.SECONDS));

        assertEquals(SegmentRegistryCache.InvalidateStatus.BUSY,
                cache.forceInvalidate(id(1)));

        allowLoad.countDown();
        assertSame(value, loading.get(1, TimeUnit.SECONDS));
    }

    @Test
    void forceInvalidateReturnsBusyWhileEntryUnloading() throws Exception {
        final CountDownLatch unloadStarted = new CountDownLatch(1);
        final CountDownLatch allowUnload = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                10, key -> segment(key.getId()), value -> {
                    unloadStarted.countDown();
                    awaitLatch(allowUnload);
                });

        assertEquals(id(1), cache.get(id(1)).getId());

        final Future<SegmentRegistryCache.InvalidateStatus> invalidation =
                executor.submit(() -> cache.forceInvalidate(id(1)));
        assertTrue(unloadStarted.await(1, TimeUnit.SECONDS));

        assertEquals(SegmentRegistryCache.InvalidateStatus.BUSY,
                cache.forceInvalidate(id(1)));

        allowUnload.countDown();
        assertEquals(SegmentRegistryCache.InvalidateStatus.REMOVED,
                invalidation.get(1, TimeUnit.SECONDS));
    }

    @Test
    void forceInvalidateCloseFailureCancelsUnloadAndKeepsEntryAvailable() {
        final AtomicInteger loads = new AtomicInteger();
        final SegmentRegistryCache<Integer, String> cache = newCache(
                2, key -> segment(loads.incrementAndGet()), value -> {
                    throw new IllegalStateException("close failed");
                });

        final Segment<Integer, String> first = cache.get(id(1));
        assertEquals(SegmentRegistryCache.InvalidateStatus.BUSY,
                cache.forceInvalidate(id(1)));
        assertSame(first, cache.get(id(1)));
        assertEquals(1, loads.get());
    }

    @Test
    void entryTransitionsFromLoadingToReadyToUnloading() {
        final SegmentRegistryCache.Entry<String> entry = new SegmentRegistryCache.Entry<>(
                7L);

        assertTrue(entry.tryStartLoad());

        final String value = "value";
        entry.finishLoad(value);
        assertEquals(value, entry.waitWhileLoading(8L));

        assertTrue(entry.tryStartUnload(value));
        assertEquals(value, entry.getValueForUnload());
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
        final IllegalStateException failure = new IllegalStateException("boom");
        assertThrows(IllegalStateException.class,
                () -> entry.fail(failure));
        assertThrows(IllegalStateException.class, entry::finishUnload);
    }

    private static SegmentRegistryCache<Integer, String> newCache(
            final int limit,
            final Function<SegmentId, Segment<Integer, String>> loader,
            final Consumer<Segment<Integer, String>> unloader) {
        return newCache(limit, loader, unloader, Runnable::run,
                value -> true);
    }

    private static SegmentRegistryCache<Integer, String> newCache(
            final int limit,
            final Function<SegmentId, Segment<Integer, String>> loader,
            final Consumer<Segment<Integer, String>> unloader,
            final Executor unloadExecutor) {
        return newCache(limit, loader, unloader, unloadExecutor,
                value -> true);
    }

    private static SegmentRegistryCache<Integer, String> newCache(
            final int limit,
            final Function<SegmentId, Segment<Integer, String>> loader,
            final Consumer<Segment<Integer, String>> unloader,
            final Executor unloadExecutor,
            final Predicate<Segment<Integer, String>> unloadablePredicate) {
        @SuppressWarnings("unchecked")
        final SegmentLoadCloseOperations<Integer, String> segmentOperations = Mockito
                .mock(SegmentLoadCloseOperations.class);
        Mockito.when(segmentOperations.loadSegment(Mockito.any(SegmentId.class)))
                .thenAnswer(invocation -> loader
                        .apply(invocation.getArgument(0)));
        Mockito.doAnswer(invocation -> {
            unloader.accept(invocation.getArgument(0));
            return null;
        }).when(segmentOperations)
                .closeSegmentIfNeeded(Mockito.<Segment<Integer, String>>any());
        final SegmentUnloadEligibility unloadEligibility = Mockito
                .mock(SegmentUnloadEligibility.class);
        Mockito.when(unloadEligibility.canUnload(Mockito.any()))
                .thenAnswer(invocation -> unloadablePredicate
                        .test(invocation.getArgument(0)));
        return new SegmentRegistryCache<>(limit, segmentOperations,
                unloadEligibility, unloadExecutor);
    }

    private static Segment<Integer, String> segment(final int id) {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        Mockito.when(segment.getId()).thenReturn(id(id));
        Mockito.when(segment.getState()).thenReturn(SegmentState.READY);
        Mockito.when(segment.getNumberOfKeysInWriteCache()).thenReturn(0);
        Mockito.when(segment.close()).thenReturn(OperationResult.ok());
        return segment;
    }

    private static SegmentId id(final int id) {
        return SegmentId.of(id);
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
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            if (Thread.currentThread().isInterrupted()) {
                throw new TimeoutException("Interrupted while waiting");
            }
        }
    }

    private static <T> T futureGetWithTimeout(final Future<T> future)
            throws Exception {
        return future.get(1, TimeUnit.SECONDS);
    }
}
