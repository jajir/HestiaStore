package org.hestiastore.index.segmentindex.core.execution;

import org.hestiastore.index.Vldtn;

/**
 * Immutable point-operation runtime statistics.
 */
public final class OperationStatsSnapshot {

    private final long getCount;
    private final long putCount;
    private final long deleteCount;
    private final long readLatencyP50Micros;
    private final long readLatencyP95Micros;
    private final long readLatencyP99Micros;
    private final long writeLatencyP50Micros;
    private final long writeLatencyP95Micros;
    private final long writeLatencyP99Micros;

    @SuppressWarnings("java:S107")
    public OperationStatsSnapshot(final long getCount, final long putCount,
            final long deleteCount, final long readLatencyP50Micros,
            final long readLatencyP95Micros,
            final long readLatencyP99Micros,
            final long writeLatencyP50Micros,
            final long writeLatencyP95Micros,
            final long writeLatencyP99Micros) {
        this.getCount = Vldtn.requireGreaterThanOrEqualToZero(getCount,
                "getCount");
        this.putCount = Vldtn.requireGreaterThanOrEqualToZero(putCount,
                "putCount");
        this.deleteCount = Vldtn.requireGreaterThanOrEqualToZero(deleteCount,
                "deleteCount");
        this.readLatencyP50Micros = Vldtn.requireGreaterThanOrEqualToZero(
                readLatencyP50Micros, "readLatencyP50Micros");
        this.readLatencyP95Micros = Vldtn.requireGreaterThanOrEqualToZero(
                readLatencyP95Micros, "readLatencyP95Micros");
        this.readLatencyP99Micros = Vldtn.requireGreaterThanOrEqualToZero(
                readLatencyP99Micros, "readLatencyP99Micros");
        this.writeLatencyP50Micros = Vldtn.requireGreaterThanOrEqualToZero(
                writeLatencyP50Micros, "writeLatencyP50Micros");
        this.writeLatencyP95Micros = Vldtn.requireGreaterThanOrEqualToZero(
                writeLatencyP95Micros, "writeLatencyP95Micros");
        this.writeLatencyP99Micros = Vldtn.requireGreaterThanOrEqualToZero(
                writeLatencyP99Micros, "writeLatencyP99Micros");
    }

    public long getGetCount() {
        return getCount;
    }

    public long getPutCount() {
        return putCount;
    }

    public long getDeleteCount() {
        return deleteCount;
    }

    public long getReadLatencyP50Micros() {
        return readLatencyP50Micros;
    }

    public long getReadLatencyP95Micros() {
        return readLatencyP95Micros;
    }

    public long getReadLatencyP99Micros() {
        return readLatencyP99Micros;
    }

    public long getWriteLatencyP50Micros() {
        return writeLatencyP50Micros;
    }

    public long getWriteLatencyP95Micros() {
        return writeLatencyP95Micros;
    }

    public long getWriteLatencyP99Micros() {
        return writeLatencyP99Micros;
    }
}
