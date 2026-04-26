package org.hestiastore.index.segmentindex.core.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * Holds runtime counters and latency distributions for index observability.
 */
public final class Stats {

    private final LongAdder putCount = new LongAdder();
    private final LongAdder getCount = new LongAdder();
    private final LongAdder deleteCount = new LongAdder();
    private final LongAdder flushRequestCount = new LongAdder();
    private final LongAdder splitScheduleCount = new LongAdder();
    private final LongAdder flushBusyRetryCount = new LongAdder();
    private final LongAdder compactBusyRetryCount = new LongAdder();
    private final StatsLatencySet latencies = new StatsLatencySet();

    public Stats() {
    }

    public void recordPutRequest() {
        putCount.increment();
    }

    public void recordGetRequest() {
        getCount.increment();
    }

    public void recordDeleteRequest() {
        deleteCount.increment();
    }

    public void recordFlushRequest() {
        flushRequestCount.increment();
    }

    public void recordSplitScheduled() {
        splitScheduleCount.increment();
    }

    public void recordFlushBusyRetry() {
        flushBusyRetryCount.increment();
    }

    public void recordCompactBusyRetry() {
        compactBusyRetryCount.increment();
    }

    public void recordReadLatencyNanos(final long nanos) {
        latencies.recordReadLatencyNanos(nanos);
    }

    public void recordWriteLatencyNanos(final long nanos) {
        latencies.recordWriteLatencyNanos(nanos);
    }

    public void recordDrainLatencyNanos(final long nanos) {
        latencies.recordDrainLatencyNanos(nanos);
    }

    public void recordSplitTaskStartDelayNanos(final long nanos) {
        latencies.recordSplitTaskStartDelayNanos(nanos);
    }

    public void recordSplitTaskRunLatencyNanos(final long nanos) {
        latencies.recordSplitTaskRunLatencyNanos(nanos);
    }

    public void recordDrainTaskStartDelayNanos(final long nanos) {
        latencies.recordDrainTaskStartDelayNanos(nanos);
    }

    public void recordDrainTaskRunLatencyNanos(final long nanos) {
        latencies.recordDrainTaskRunLatencyNanos(nanos);
    }

    public void recordFlushAcceptedToReadyNanos(final long nanos) {
        latencies.recordFlushAcceptedToReadyNanos(nanos);
    }

    public void recordCompactAcceptedToReadyNanos(final long nanos) {
        latencies.recordCompactAcceptedToReadyNanos(nanos);
    }

    public long getPutCount() {
        return putCount.sum();
    }

    public long getGetCount() {
        return getCount.sum();
    }

    public long getDeleteCount() {
        return deleteCount.sum();
    }

    public long getFlushRequestCount() {
        return flushRequestCount.sum();
    }

    public long getSplitScheduleCount() {
        return splitScheduleCount.sum();
    }

    public long getFlushBusyRetryCount() {
        return flushBusyRetryCount.sum();
    }

    public long getCompactBusyRetryCount() {
        return compactBusyRetryCount.sum();
    }

    public long getReadLatencyP50Micros() {
        return latencies.getReadLatencyP50Micros();
    }

    public long getReadLatencyP95Micros() {
        return latencies.getReadLatencyP95Micros();
    }

    public long getReadLatencyP99Micros() {
        return latencies.getReadLatencyP99Micros();
    }

    public long getWriteLatencyP50Micros() {
        return latencies.getWriteLatencyP50Micros();
    }

    public long getWriteLatencyP95Micros() {
        return latencies.getWriteLatencyP95Micros();
    }

    public long getWriteLatencyP99Micros() {
        return latencies.getWriteLatencyP99Micros();
    }

    public long getDrainLatencyP95Micros() {
        return latencies.getDrainLatencyP95Micros();
    }

    public long getSplitTaskStartDelayP95Micros() {
        return latencies.getSplitTaskStartDelayP95Micros();
    }

    public long getSplitTaskRunLatencyP95Micros() {
        return latencies.getSplitTaskRunLatencyP95Micros();
    }

    public long getDrainTaskStartDelayP95Micros() {
        return latencies.getDrainTaskStartDelayP95Micros();
    }

    public long getDrainTaskRunLatencyP95Micros() {
        return latencies.getDrainTaskRunLatencyP95Micros();
    }

    public long getFlushAcceptedToReadyP95Micros() {
        return latencies.getFlushAcceptedToReadyP95Micros();
    }

    public long getCompactAcceptedToReadyP95Micros() {
        return latencies.getCompactAcceptedToReadyP95Micros();
    }
}
