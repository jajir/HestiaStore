package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.atomic.LongAdder;

/**
 * Holds statistic informations about index utilization.
 * 
 * @author honza
 *
 */
class Stats {
    private final LongAdder putCx = new LongAdder();
    private final LongAdder getCx = new LongAdder();
    private final LongAdder deleteCx = new LongAdder();
    private final LongAdder compactRequestCx = new LongAdder();
    private final LongAdder flushRequestCx = new LongAdder();
    private final LongAdder splitScheduleCx = new LongAdder();
    private final LongAdder putBusyRetryCx = new LongAdder();
    private final LongAdder putBusyTimeoutCx = new LongAdder();
    private final LongAdder flushBusyRetryCx = new LongAdder();
    private final LongAdder compactBusyRetryCx = new LongAdder();
    private final OperationLatencyTracker readLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker writeLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker drainLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker putBusyWaitLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker splitTaskStartDelayLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker splitTaskRunLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker drainTaskStartDelayLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker drainTaskRunLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker flushAcceptedToReadyLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker compactAcceptedToReadyLatency = new OperationLatencyTracker();

    Stats() {

    }

    void incPutCx() {
        putCx.increment();
    }

    void incGetCx() {
        getCx.increment();
    }

    void incDeleteCx() {
        deleteCx.increment();
    }

    void incCompactRequestCx() {
        compactRequestCx.increment();
    }

    void incFlushRequestCx() {
        flushRequestCx.increment();
    }

    void incSplitScheduleCx() {
        splitScheduleCx.increment();
    }

    void addPutBusyRetryCx(final long retries) {
        if (retries <= 0L) {
            return;
        }
        putBusyRetryCx.add(retries);
    }

    void incPutBusyTimeoutCx() {
        putBusyTimeoutCx.increment();
    }

    void incFlushBusyRetryCx() {
        flushBusyRetryCx.increment();
    }

    void incCompactBusyRetryCx() {
        compactBusyRetryCx.increment();
    }

    void recordReadLatencyNanos(final long nanos) {
        readLatency.recordNanos(nanos);
    }

    void recordWriteLatencyNanos(final long nanos) {
        writeLatency.recordNanos(nanos);
    }

    void recordDrainLatencyNanos(final long nanos) {
        drainLatency.recordNanos(nanos);
    }

    void recordPutBusyWaitNanos(final long nanos) {
        putBusyWaitLatency.recordNanos(nanos);
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

    /**
     * Returns the number of put operations recorded.
     *
     * @return put count
     */
    public long getPutCx() {
        return putCx.sum();
    }

    /**
     * Returns the number of get operations recorded.
     *
     * @return get count
     */
    public long getGetCx() {
        return getCx.sum();
    }

    /**
     * Returns the number of delete operations recorded.
     *
     * @return delete count
     */
    public long getDeleteCx() {
        return deleteCx.sum();
    }

    long getCompactRequestCx() {
        return compactRequestCx.sum();
    }

    long getFlushRequestCx() {
        return flushRequestCx.sum();
    }

    long getSplitScheduleCx() {
        return splitScheduleCx.sum();
    }

    long getPutBusyRetryCx() {
        return putBusyRetryCx.sum();
    }

    long getPutBusyTimeoutCx() {
        return putBusyTimeoutCx.sum();
    }

    long getFlushBusyRetryCx() {
        return flushBusyRetryCx.sum();
    }

    long getCompactBusyRetryCx() {
        return compactBusyRetryCx.sum();
    }

    long getReadLatencyP50Micros() {
        return readLatency.percentileMicros(0.50D);
    }

    long getReadLatencyP95Micros() {
        return readLatency.percentileMicros(0.95D);
    }

    long getReadLatencyP99Micros() {
        return readLatency.percentileMicros(0.99D);
    }

    long getWriteLatencyP50Micros() {
        return writeLatency.percentileMicros(0.50D);
    }

    long getWriteLatencyP95Micros() {
        return writeLatency.percentileMicros(0.95D);
    }

    long getWriteLatencyP99Micros() {
        return writeLatency.percentileMicros(0.99D);
    }

    long getDrainLatencyP95Micros() {
        return drainLatency.percentileMicros(0.95D);
    }

    long getPutBusyWaitP95Micros() {
        return putBusyWaitLatency.percentileMicros(0.95D);
    }

    long getSplitTaskStartDelayP95Micros() {
        return splitTaskStartDelayLatency.percentileMicros(0.95D);
    }

    long getSplitTaskRunLatencyP95Micros() {
        return splitTaskRunLatency.percentileMicros(0.95D);
    }

    long getDrainTaskStartDelayP95Micros() {
        return drainTaskStartDelayLatency.percentileMicros(0.95D);
    }

    long getDrainTaskRunLatencyP95Micros() {
        return drainTaskRunLatency.percentileMicros(0.95D);
    }

    long getFlushAcceptedToReadyP95Micros() {
        return flushAcceptedToReadyLatency.percentileMicros(0.95D);
    }

    long getCompactAcceptedToReadyP95Micros() {
        return compactAcceptedToReadyLatency.percentileMicros(0.95D);
    }

}
