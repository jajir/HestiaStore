package org.hestiastore.index.segmentindex;

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
    private final OperationLatencyTracker readLatency = new OperationLatencyTracker();
    private final OperationLatencyTracker writeLatency = new OperationLatencyTracker();

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

    void recordReadLatencyNanos(final long nanos) {
        readLatency.recordNanos(nanos);
    }

    void recordWriteLatencyNanos(final long nanos) {
        writeLatency.recordNanos(nanos);
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

}
