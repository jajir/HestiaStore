package org.hestiastore.index.segmentindex.core.metrics;

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

        stats.recordPutRequest();
        stats.recordPutRequest();
        stats.recordGetRequest();
        stats.recordDeleteRequest();
        stats.recordDeleteRequest();
        stats.recordDeleteRequest();

        assertEquals(2, stats.getPutCount());
        assertEquals(1, stats.getGetCount());
        assertEquals(3, stats.getDeleteCount());
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
                            stats.recordPutRequest();
                            stats.recordGetRequest();
                            stats.recordDeleteRequest();
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
        assertEquals(expected, stats.getPutCount());
        assertEquals(expected, stats.getGetCount());
        assertEquals(expected, stats.getDeleteCount());
    }

    @Test
    void statsExposeRecordedLatenciesAndMaintenanceBusyCounters() {
        final Stats stats = new Stats();

        stats.recordFlushBusyRetry();
        stats.recordCompactBusyRetry();
        stats.recordReadLatencyNanos(2_000L);
        stats.recordWriteLatencyNanos(3_000L);
        stats.recordDrainLatencyNanos(4_000L);
        stats.recordSplitTaskStartDelayNanos(6_000L);
        stats.recordSplitTaskRunLatencyNanos(7_000L);
        stats.recordDrainTaskStartDelayNanos(8_000L);
        stats.recordDrainTaskRunLatencyNanos(9_000L);
        stats.recordFlushAcceptedToReadyNanos(10_000L);
        stats.recordCompactAcceptedToReadyNanos(11_000L);

        assertEquals(1L, stats.getFlushBusyRetryCount());
        assertEquals(1L, stats.getCompactBusyRetryCount());
        assertTrue(stats.getReadLatencyP50Micros() >= 2L);
        assertTrue(stats.getWriteLatencyP95Micros() >= 3L);
        assertTrue(stats.getDrainLatencyP95Micros() >= 4L);
        assertTrue(stats.getSplitTaskStartDelayP95Micros() >= 6L);
        assertTrue(stats.getSplitTaskRunLatencyP95Micros() >= 7L);
        assertTrue(stats.getDrainTaskStartDelayP95Micros() >= 8L);
        assertTrue(stats.getDrainTaskRunLatencyP95Micros() >= 9L);
        assertTrue(stats.getFlushAcceptedToReadyP95Micros() >= 10L);
        assertTrue(stats.getCompactAcceptedToReadyP95Micros() >= 11L);
    }
}
