package org.hestiastore.index.segmentindex.monitoring.model;

/**
 * User-facing operation latency percentiles.
 */
public final class SegmentIndexLatencyMetrics {

    private final long readP50Micros;
    private final long readP95Micros;
    private final long readP99Micros;
    private final long writeP50Micros;
    private final long writeP95Micros;
    private final long writeP99Micros;

    /**
     * Creates latency metrics.
     *
     * @param readP50Micros read p50 latency in microseconds
     * @param readP95Micros read p95 latency in microseconds
     * @param readP99Micros read p99 latency in microseconds
     * @param writeP50Micros write p50 latency in microseconds
     * @param writeP95Micros write p95 latency in microseconds
     * @param writeP99Micros write p99 latency in microseconds
     */
    public SegmentIndexLatencyMetrics(final long readP50Micros,
            final long readP95Micros, final long readP99Micros,
            final long writeP50Micros, final long writeP95Micros,
            final long writeP99Micros) {
        this.readP50Micros = MetricModelValidation.nonNegative(readP50Micros,
                "readP50Micros");
        this.readP95Micros = MetricModelValidation.nonNegative(readP95Micros,
                "readP95Micros");
        this.readP99Micros = MetricModelValidation.nonNegative(readP99Micros,
                "readP99Micros");
        this.writeP50Micros = MetricModelValidation.nonNegative(
                writeP50Micros, "writeP50Micros");
        this.writeP95Micros = MetricModelValidation.nonNegative(
                writeP95Micros, "writeP95Micros");
        this.writeP99Micros = MetricModelValidation.nonNegative(
                writeP99Micros, "writeP99Micros");
    }

    /**
     * Returns read p50 latency in microseconds.
     *
     * @return read p50 latency
     */
    public long readP50Micros() {
        return readP50Micros;
    }

    /**
     * Returns read p95 latency in microseconds.
     *
     * @return read p95 latency
     */
    public long readP95Micros() {
        return readP95Micros;
    }

    /**
     * Returns read p99 latency in microseconds.
     *
     * @return read p99 latency
     */
    public long readP99Micros() {
        return readP99Micros;
    }

    /**
     * Returns write p50 latency in microseconds.
     *
     * @return write p50 latency
     */
    public long writeP50Micros() {
        return writeP50Micros;
    }

    /**
     * Returns write p95 latency in microseconds.
     *
     * @return write p95 latency
     */
    public long writeP95Micros() {
        return writeP95Micros;
    }

    /**
     * Returns write p99 latency in microseconds.
     *
     * @return write p99 latency
     */
    public long writeP99Micros() {
        return writeP99Micros;
    }
}
