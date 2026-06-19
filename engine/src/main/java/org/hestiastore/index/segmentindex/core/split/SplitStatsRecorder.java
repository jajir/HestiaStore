package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.segmentindex.core.OperationLatencyTracker;

/**
 * Mutable recorder for split runtime statistics.
 */
public final class SplitStatsRecorder {

    private static final double PERCENTILE_95 = 0.95D;

    private final LongAdder splitScheduleCount = new LongAdder();
    private final OperationLatencyTracker splitTaskStartDelayLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker splitTaskRunLatency =
            new OperationLatencyTracker();

    /**
     * Creates an empty split stats recorder.
     */
    public SplitStatsRecorder() {
    }

    /**
     * Records that a split candidate was scheduled.
     */
    public void recordSplitScheduled() {
        splitScheduleCount.increment();
    }

    /**
     * Records scheduler-to-start latency for a split task.
     *
     * @param nanos latency in nanoseconds
     */
    public void recordSplitTaskStartDelayNanos(final long nanos) {
        splitTaskStartDelayLatency.recordNanos(nanos);
    }

    /**
     * Records runtime latency for a split task.
     *
     * @param nanos runtime in nanoseconds
     */
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
