package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

class SegmentRegistryConcurrencyStressTest {

    private static final int WAIT_SECONDS = 20;

    @Test
    @Timeout(20)
    void sameKeySingleFlight_underHighContention() throws Exception {
        final AtomicInteger loaderCalls = new AtomicInteger();
        final CountDownLatch loaderStarted = new CountDownLatch(1);
        final CountDownLatch allowLoadFinish = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                32, key -> {
                    loaderCalls.incrementAndGet();
                    loaderStarted.countDown();
                    awaitLatch(allowLoadFinish);
                    return segment(key.getId());
                });

        final int threadCount = 24;
        final ExecutorService executor = Executors.newFixedThreadPool(
                threadCount);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<Segment<Integer, String>>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(() -> {
                    awaitLatch(start);
                    return cache.get(id(7));
                }));
            }

            start.countDown();
            assertTrue(loaderStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            allowLoadFinish.countDown();

            for (final Future<Segment<Integer, String>> future : futures) {
                assertEquals(id(7),
                        future.get(WAIT_SECONDS, TimeUnit.SECONDS).getId());
            }
            assertEquals(1, loaderCalls.get(),
                    "Same-key contention must execute loader exactly once.");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(20)
    void differentKeys_doNotBlockEachOther() throws Exception {
        final CountDownLatch keyOneLoaderStarted = new CountDownLatch(1);
        final CountDownLatch allowKeyOneFinish = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = newCache(
                32, key -> {
                    if (key.equals(id(1))) {
                        keyOneLoaderStarted.countDown();
                        awaitLatch(allowKeyOneFinish);
                    }
                    return segment(key.getId());
                });

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<Segment<Integer, String>> blockedKey = executor
                    .submit(() -> cache.get(id(1)));
            assertTrue(keyOneLoaderStarted.await(WAIT_SECONDS,
                    TimeUnit.SECONDS));

            final long startNanos = System.nanoTime();
            final Segment<Integer, String> differentKeyValue = cache
                    .get(id(2));
            final long elapsedMillis = TimeUnit.NANOSECONDS
                    .toMillis(System.nanoTime() - startNanos);

            assertEquals(id(2), differentKeyValue.getId());
            assertTrue(elapsedMillis < 250,
                    "Different key must not wait for another key load.");

            allowKeyOneFinish.countDown();
            assertEquals(id(1),
                    blockedKey.get(WAIT_SECONDS, TimeUnit.SECONDS).getId());
        } finally {
            executor.shutdownNow();
        }
    }

    private static void awaitLatch(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting", e);
        }
    }

    private static SegmentRegistryCache<Integer, String> newCache(
            final int limit,
            final Function<SegmentId, Segment<Integer, String>> loader) {
        @SuppressWarnings("unchecked")
        final SegmentLoadCloseOperations<Integer, String> segmentOperations = Mockito
                .mock(SegmentLoadCloseOperations.class);
        Mockito.when(segmentOperations.loadSegment(Mockito.any(SegmentId.class)))
                .thenAnswer(invocation -> loader
                        .apply(invocation.getArgument(0)));
        Mockito.doAnswer(invocation -> null).when(segmentOperations)
                .closeSegmentIfNeeded(Mockito.<Segment<Integer, String>>any());
        final SegmentUnloadEligibility unloadEligibility = Mockito
                .mock(SegmentUnloadEligibility.class);
        Mockito.when(unloadEligibility.canUnload(Mockito.any()))
                .thenReturn(true);
        return new SegmentRegistryCache<>(limit, segmentOperations,
                unloadEligibility, Runnable::run);
    }

    private static Segment<Integer, String> segment(final int id) {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        Mockito.when(segment.getId()).thenReturn(id(id));
        Mockito.when(segment.getState()).thenReturn(SegmentState.READY);
        Mockito.when(segment.getNumberOfKeysInWriteCache()).thenReturn(0);
        return segment;
    }

    private static SegmentId id(final int id) {
        return SegmentId.of(id);
    }
}
