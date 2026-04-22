package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.OperationStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.junit.jupiter.api.Test;

class PutBusyRetryMonitorTest {

    @Test
    void finishRecordsPutBusyWaitMetrics() {
        final Stats stats = new Stats();
        final PutBusyRetryMonitor monitor = new PutBusyRetryMonitor("put",
                stats, sequenceNanoTimeSupplier(10_000L, 35_000L));

        monitor.observeRetryableStatus(OperationStatus.BUSY);
        monitor.finishWithoutFailure();

        assertEquals(1L, stats.getPutBusyRetryCount());
        assertEquals(25L, stats.getPutBusyWaitP95Micros());
        assertEquals(0L, stats.getPutBusyTimeoutCount());
    }

    @Test
    void timeoutFailureIncrementsPutBusyTimeoutCounter() {
        final Stats stats = new Stats();
        final PutBusyRetryMonitor monitor = new PutBusyRetryMonitor("put",
                stats, sequenceNanoTimeSupplier(20_000L, 52_000L));

        monitor.observeRetryableStatus(OperationStatus.BUSY);
        monitor.finish(new IndexException(
                "Index operation 'put' timed out after 30 ms"));

        assertEquals(1L, stats.getPutBusyRetryCount());
        assertEquals(32L, stats.getPutBusyWaitP95Micros());
        assertEquals(1L, stats.getPutBusyTimeoutCount());
    }

    @Test
    void nonPutOperationsDoNotRecordPutBusyWaitMetrics() {
        final Stats stats = new Stats();
        final PutBusyRetryMonitor monitor = new PutBusyRetryMonitor("delete",
                stats, sequenceNanoTimeSupplier(10_000L, 35_000L));

        monitor.observeRetryableStatus(OperationStatus.BUSY);
        monitor.finishWithoutFailure();

        assertEquals(0L, stats.getPutBusyRetryCount());
        assertEquals(0L, stats.getPutBusyWaitP95Micros());
        assertEquals(0L, stats.getPutBusyTimeoutCount());
    }

    private static LongSupplier sequenceNanoTimeSupplier(
            final long... nanos) {
        final AtomicInteger index = new AtomicInteger();
        return () -> {
            final int current = index.getAndIncrement();
            final int safeIndex = Math.min(current, nanos.length - 1);
            return nanos[safeIndex];
        };
    }
}
