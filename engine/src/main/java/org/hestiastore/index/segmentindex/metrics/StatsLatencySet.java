package org.hestiastore.index.segmentindex.metrics;

/**
 * Groups latency trackers used by index runtime statistics.
 */
final class StatsLatencySet {

    private static final double PERCENTILE_50 = 0.50D;
    private static final double PERCENTILE_95 = 0.95D;
    private static final double PERCENTILE_99 = 0.99D;

    private final OperationLatencyTracker readLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker writeLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker splitTaskStartDelayLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker splitTaskRunLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker drainTaskStartDelayLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker drainTaskRunLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker flushAcceptedToReadyLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker compactAcceptedToReadyLatency =
            new OperationLatencyTracker();

    void recordReadLatencyNanos(final long nanos) {
        readLatency.recordNanos(nanos);
    }

    void recordWriteLatencyNanos(final long nanos) {
        writeLatency.recordNanos(nanos);
    }

    void recordSplitTaskStartDelayNanos(final long nanos) {
        splitTaskStartDelayLatency.recordNanos(nanos);
    }

    void recordSplitTaskRunLatencyNanos(final long nanos) {
        splitTaskRunLatency.recordNanos(nanos);
    }

    void recordDrainTaskStartDelayNanos(final long nanos) {
        drainTaskStartDelayLatency.recordNanos(nanos);
    }

    void recordDrainTaskRunLatencyNanos(final long nanos) {
        drainTaskRunLatency.recordNanos(nanos);
    }

    void recordFlushAcceptedToReadyNanos(final long nanos) {
        flushAcceptedToReadyLatency.recordNanos(nanos);
    }

    void recordCompactAcceptedToReadyNanos(final long nanos) {
        compactAcceptedToReadyLatency.recordNanos(nanos);
    }

    long getReadLatencyP50Micros() {
        return readLatency.percentileMicros(PERCENTILE_50);
    }

    long getReadLatencyP95Micros() {
        return readLatency.percentileMicros(PERCENTILE_95);
    }

    long getReadLatencyP99Micros() {
        return readLatency.percentileMicros(PERCENTILE_99);
    }

    long getWriteLatencyP50Micros() {
        return writeLatency.percentileMicros(PERCENTILE_50);
    }

    long getWriteLatencyP95Micros() {
        return writeLatency.percentileMicros(PERCENTILE_95);
    }

    long getWriteLatencyP99Micros() {
        return writeLatency.percentileMicros(PERCENTILE_99);
    }

    long getSplitTaskStartDelayP95Micros() {
        return splitTaskStartDelayLatency.percentileMicros(PERCENTILE_95);
    }

    long getSplitTaskRunLatencyP95Micros() {
        return splitTaskRunLatency.percentileMicros(PERCENTILE_95);
    }

    long getDrainTaskStartDelayP95Micros() {
        return drainTaskStartDelayLatency.percentileMicros(PERCENTILE_95);
    }

    long getDrainTaskRunLatencyP95Micros() {
        return drainTaskRunLatency.percentileMicros(PERCENTILE_95);
    }

    long getFlushAcceptedToReadyP95Micros() {
        return flushAcceptedToReadyLatency.percentileMicros(PERCENTILE_95);
    }

    long getCompactAcceptedToReadyP95Micros() {
        return compactAcceptedToReadyLatency.percentileMicros(PERCENTILE_95);
    }
}
