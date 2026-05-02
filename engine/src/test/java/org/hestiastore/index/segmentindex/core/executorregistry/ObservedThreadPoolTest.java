package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.segmentindex.metrics.IndexExecutorMetricsAccess;
import org.junit.jupiter.api.Test;

class ObservedThreadPoolTest {

    @Test
    void snapshotUsesExecutorAndCounterState() {
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(4));
        final LongAdder rejectedTaskCount = new LongAdder();
        final LongAdder callerRunsCount = new LongAdder();
        rejectedTaskCount.add(2L);
        callerRunsCount.add(3L);
        final ObservedThreadPool observedThreadPool = new ObservedThreadPool(
                executor, 4, rejectedTaskCount, callerRunsCount);
        try {
            executor.execute(() -> {
            });
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.SECONDS);

            final IndexExecutorMetricsAccess snapshot =
                    observedThreadPool.snapshot();

            assertEquals(0, snapshot.getActiveThreadCount());
            assertEquals(0, snapshot.getQueueSize());
            assertEquals(4, snapshot.getQueueCapacity());
            assertEquals(1L, snapshot.getCompletedTaskCount());
            assertEquals(2L, snapshot.getRejectedTaskCount());
            assertEquals(3L, snapshot.getCallerRunsCount());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for observed thread pool test executor.",
                    e);
        } finally {
            executor.shutdownNow();
        }
    }
}
