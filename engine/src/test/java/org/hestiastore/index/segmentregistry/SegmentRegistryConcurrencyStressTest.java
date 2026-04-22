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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SegmentRegistryConcurrencyStressTest {

    private static final int WAIT_SECONDS = 20;

    @Test
    @Timeout(20)
    void sameKeySingleFlight_underHighContention() throws Exception {
        final AtomicInteger loaderCalls = new AtomicInteger();
        final CountDownLatch loaderStarted = new CountDownLatch(1);
        final CountDownLatch allowLoadFinish = new CountDownLatch(1);
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                32, key -> {
                    loaderCalls.incrementAndGet();
                    loaderStarted.countDown();
                    awaitLatch(allowLoadFinish);
                    return "v-" + key;
                }, value -> {
                });

        final int threadCount = 24;
        final ExecutorService executor = Executors.newFixedThreadPool(
                threadCount);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(() -> {
                    awaitLatch(start);
                    return cache.get(7);
                }));
            }

            start.countDown();
            assertTrue(loaderStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            allowLoadFinish.countDown();

            for (final Future<String> future : futures) {
                assertEquals("v-7", future.get(WAIT_SECONDS,
                        TimeUnit.SECONDS));
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
        final SegmentRegistryCache<Integer, String> cache = new SegmentRegistryCache<>(
                32, key -> {
                    if (key.intValue() == 1) {
                        keyOneLoaderStarted.countDown();
                        awaitLatch(allowKeyOneFinish);
                    }
                    return "v-" + key;
                }, value -> {
                });

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<String> blockedKey = executor
                    .submit(() -> cache.get(1));
            assertTrue(keyOneLoaderStarted.await(WAIT_SECONDS,
                    TimeUnit.SECONDS));

            final long startNanos = System.nanoTime();
            final String differentKeyValue = cache.get(2).orElse(null);
            final long elapsedMillis = TimeUnit.NANOSECONDS
                    .toMillis(System.nanoTime() - startNanos);

            assertEquals("v-2", differentKeyValue);
            assertTrue(elapsedMillis < 250,
                    "Different key must not wait for another key load.");

            allowKeyOneFinish.countDown();
            assertEquals("v-1",
                    blockedKey.get(WAIT_SECONDS, TimeUnit.SECONDS));
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
}
