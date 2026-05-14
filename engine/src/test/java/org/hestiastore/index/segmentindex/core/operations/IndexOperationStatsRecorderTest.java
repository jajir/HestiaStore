package org.hestiastore.index.segmentindex.core.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class IndexOperationStatsRecorderTest {

    @Test
    void countersIncrementCorrectly() {
        final IndexOperationStatsRecorder recorder =
                new IndexOperationStatsRecorder();

        recorder.recordPutRequest();
        recorder.recordPutRequest();
        recorder.recordGetRequest();
        recorder.recordDeleteRequest();
        recorder.recordDeleteRequest();
        recorder.recordDeleteRequest();

        final IndexOperationStats stats = recorder.statsSnapshot();
        assertEquals(2, stats.getPutCount());
        assertEquals(1, stats.getGetCount());
        assertEquals(3, stats.getDeleteCount());
    }

    @Test
    void countersAreThreadSafe() throws InterruptedException {
        final IndexOperationStatsRecorder recorder =
                new IndexOperationStatsRecorder();
        final int threads = 8;
        final int increments = 1_000;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int i = 0; i < threads; i++) {
                executor.execute(() -> recordOperations(recorder, increments,
                        ready, start, done));
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
        final IndexOperationStats stats = recorder.statsSnapshot();
        assertEquals(expected, stats.getPutCount());
        assertEquals(expected, stats.getGetCount());
        assertEquals(expected, stats.getDeleteCount());
    }

    @Test
    void statsExposeRecordedLatencies() {
        final IndexOperationStatsRecorder recorder =
                new IndexOperationStatsRecorder();

        recorder.recordReadLatencyNanos(2_000L);
        recorder.recordWriteLatencyNanos(3_000L);

        final IndexOperationStats stats = recorder.statsSnapshot();
        assertTrue(stats.getReadLatencyP50Micros() >= 2L);
        assertTrue(stats.getReadLatencyP95Micros() >= 2L);
        assertTrue(stats.getReadLatencyP99Micros() >= 2L);
        assertTrue(stats.getWriteLatencyP50Micros() >= 3L);
        assertTrue(stats.getWriteLatencyP95Micros() >= 3L);
        assertTrue(stats.getWriteLatencyP99Micros() >= 3L);
    }

    private static void recordOperations(
            final IndexOperationStatsRecorder recorder, final int increments,
            final CountDownLatch ready, final CountDownLatch start,
            final CountDownLatch done) {
        ready.countDown();
        try {
            start.await();
            for (int j = 0; j < increments; j++) {
                recorder.recordPutRequest();
                recorder.recordGetRequest();
                recorder.recordDeleteRequest();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            done.countDown();
        }
    }
}
