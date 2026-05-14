package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.segmentindex.core.telemetry.OperationLatencyTracker;

/**
 * Mutable recorder for split runtime statistics.
 */
public final class SplitStatsRecorder implements SplitTelemetry {

    private static final double PERCENTILE_95 = 0.95D;

    private final LongAdder splitScheduleCount = new LongAdder();
    private final OperationLatencyTracker splitTaskStartDelayLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker splitTaskRunLatency =
            new OperationLatencyTracker();

    public SplitStatsRecorder() {
    }

    @Override
    public void recordSplitScheduled() {
        splitScheduleCount.increment();
    }

    @Override
    public void recordSplitTaskStartDelayNanos(final long nanos) {
        splitTaskStartDelayLatency.recordNanos(nanos);
    }

    @Override
    public void recordSplitTaskRunLatencyNanos(final long nanos) {
        splitTaskRunLatency.recordNanos(nanos);
    }

    SplitStats statsSnapshot(final int splitInFlightCount,
            final int splitBlockedCount) {
        return new SplitStats(splitScheduleCount.sum(), splitInFlightCount,
                splitBlockedCount,
                splitTaskStartDelayLatency.percentileMicros(PERCENTILE_95),
                splitTaskRunLatency.percentileMicros(PERCENTILE_95));
    }
}
