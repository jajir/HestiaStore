package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.segmentindex.core.OperationLatencyTracker;

/**
 * Mutable recorder for maintenance runtime statistics.
 */
public final class MaintenanceStatsRecorder {

    private static final double PERCENTILE_95 = 0.95D;

    private final LongAdder flushRequestCount = new LongAdder();
    private final LongAdder compactRequestCount = new LongAdder();
    private final LongAdder flushBusyRetryCount = new LongAdder();
    private final LongAdder compactBusyRetryCount = new LongAdder();
    private final OperationLatencyTracker flushAcceptedToReadyLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker compactAcceptedToReadyLatency =
            new OperationLatencyTracker();

    public MaintenanceStatsRecorder() {
    }

    public void recordFlushRequest() {
        flushRequestCount.increment();
    }

    public void recordCompactRequest() {
        compactRequestCount.increment();
    }

    public void recordFlushBusyRetry() {
        flushBusyRetryCount.increment();
    }

    public void recordCompactBusyRetry() {
        compactBusyRetryCount.increment();
    }

    public void recordFlushAcceptedToReadyNanos(final long nanos) {
        flushAcceptedToReadyLatency.recordNanos(nanos);
    }

    public void recordCompactAcceptedToReadyNanos(final long nanos) {
        compactAcceptedToReadyLatency.recordNanos(nanos);
    }

    public MaintenanceStats statsSnapshot() {
        return new MaintenanceStats(flushRequestCount.sum(),
                compactRequestCount.sum(), flushBusyRetryCount.sum(),
                compactBusyRetryCount.sum(),
                flushAcceptedToReadyLatency.percentileMicros(PERCENTILE_95),
                compactAcceptedToReadyLatency.percentileMicros(PERCENTILE_95));
    }
}
