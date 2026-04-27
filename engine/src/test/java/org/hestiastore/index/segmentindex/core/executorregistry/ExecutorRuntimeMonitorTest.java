package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorRuntimeAccess;
import org.junit.jupiter.api.Test;

class ExecutorRuntimeMonitorTest {

    @Test
    void runtimeSnapshotUsesObservedPools() {
        final ExecutorRuntimeMonitor runtimeMonitor =
                new ExecutorRuntimeMonitor(
                        observedThreadPool(4, 1L, 2L),
                        observedThreadPool(8, 3L, 4L),
                        observedThreadPool(16, 5L, 6L));

        final IndexExecutorRuntimeAccess snapshot = runtimeMonitor
                .runtimeSnapshot();

        assertEquals(4, snapshot.getIndexMaintenance().getQueueCapacity());
        assertEquals(1L,
                snapshot.getIndexMaintenance().getRejectedTaskCount());
        assertEquals(2L, snapshot.getIndexMaintenance().getCallerRunsCount());
        assertEquals(8, snapshot.getSplitMaintenance().getQueueCapacity());
        assertEquals(3L, snapshot.getSplitMaintenance().getRejectedTaskCount());
        assertEquals(16,
                snapshot.getStableSegmentMaintenance().getQueueCapacity());
        assertEquals(6L, snapshot.getStableSegmentMaintenance()
                .getCallerRunsCount());
    }

    private static ObservedThreadPool observedThreadPool(
            final int queueCapacity, final long rejectedTaskCount,
            final long callerRunsCount) {
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueCapacity));
        final LongAdder rejectedTaskCounter = new LongAdder();
        final LongAdder callerRunsCounter = new LongAdder();
        rejectedTaskCounter.add(rejectedTaskCount);
        callerRunsCounter.add(callerRunsCount);
        return new ObservedThreadPool(executor, queueCapacity,
                rejectedTaskCounter, callerRunsCounter);
    }
}
