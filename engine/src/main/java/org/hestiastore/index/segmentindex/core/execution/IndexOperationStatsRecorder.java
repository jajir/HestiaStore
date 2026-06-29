package org.hestiastore.index.segmentindex.core.execution;

import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.segmentindex.core.OperationLatencyTracker;

/**
 * Mutable recorder for point-operation runtime statistics.
 */
public final class IndexOperationStatsRecorder {

    private static final double PERCENTILE_50 = 0.50D;
    private static final double PERCENTILE_95 = 0.95D;
    private static final double PERCENTILE_99 = 0.99D;

    private final LongAdder putCount = new LongAdder();
    private final LongAdder getCount = new LongAdder();
    private final LongAdder deleteCount = new LongAdder();
    private final OperationLatencyTracker readLatency =
            new OperationLatencyTracker();
    private final OperationLatencyTracker writeLatency =
            new OperationLatencyTracker();

    public IndexOperationStatsRecorder() {
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

    public void recordReadLatencyNanos(final long nanos) {
        readLatency.recordNanos(nanos);
    }

    public void recordWriteLatencyNanos(final long nanos) {
        writeLatency.recordNanos(nanos);
    }

    public OperationStatsSnapshot statsSnapshot() {
        return new OperationStatsSnapshot(getCount.sum(), putCount.sum(),
                deleteCount.sum(), readLatency.percentileMicros(PERCENTILE_50),
                readLatency.percentileMicros(PERCENTILE_95),
                readLatency.percentileMicros(PERCENTILE_99),
                writeLatency.percentileMicros(PERCENTILE_50),
                writeLatency.percentileMicros(PERCENTILE_95),
                writeLatency.percentileMicros(PERCENTILE_99));
    }
}
