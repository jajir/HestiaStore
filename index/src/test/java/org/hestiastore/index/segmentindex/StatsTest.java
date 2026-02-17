package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class StatsTest {

    @Test
    void countersIncrementCorrectly() {
        final Stats stats = new Stats();

        stats.incPutCx();
        stats.incPutCx();
        stats.incGetCx();
        stats.incDeleteCx();
        stats.incDeleteCx();
        stats.incDeleteCx();

        assertEquals(2, stats.getPutCx());
        assertEquals(1, stats.getGetCx());
        assertEquals(3, stats.getDeleteCx());
    }

    @Test
    void countersAreThreadSafe() throws Exception {
        final Stats stats = new Stats();
        final int threads = 8;
        final int increments = 1_000;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int i = 0; i < threads; i++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int j = 0; j < increments; j++) {
                            stats.incPutCx();
                            stats.incGetCx();
                            stats.incDeleteCx();
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

        final long expected = (long) threads * increments;
        assertEquals(expected, stats.getPutCx());
        assertEquals(expected, stats.getGetCx());
        assertEquals(expected, stats.getDeleteCx());
    }
}
