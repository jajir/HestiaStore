package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Latency metrics section inside an index report payload.
 */
@SuppressWarnings("java:S6206")
public final class LatencyReportResponse {

    private final long readP50Micros;
    private final long readP95Micros;
    private final long readP99Micros;
    private final long writeP50Micros;
    private final long writeP95Micros;
    private final long writeP99Micros;

    /**
     * Creates latency metrics.
     *
     * @param readP50Micros read p50 latency
     * @param readP95Micros read p95 latency
     * @param readP99Micros read p99 latency
     * @param writeP50Micros write p50 latency
     * @param writeP95Micros write p95 latency
     * @param writeP99Micros write p99 latency
     */
    @ConstructorProperties({ "readP50Micros", "readP95Micros",
            "readP99Micros", "writeP50Micros", "writeP95Micros",
            "writeP99Micros" })
    public LatencyReportResponse(final long readP50Micros,
            final long readP95Micros, final long readP99Micros,
            final long writeP50Micros, final long writeP95Micros,
            final long writeP99Micros) {
        this.readP50Micros = readP50Micros;
        this.readP95Micros = readP95Micros;
        this.readP99Micros = readP99Micros;
        this.writeP50Micros = writeP50Micros;
        this.writeP95Micros = writeP95Micros;
        this.writeP99Micros = writeP99Micros;
    }

    /**
     * Returns read p50 latency.
     *
     * @return read p50 latency
     */
    public long readP50Micros() {
        return readP50Micros;
    }

    /**
     * Returns read p95 latency.
     *
     * @return read p95 latency
     */
    public long readP95Micros() {
        return readP95Micros;
    }

    /**
     * Returns read p99 latency.
     *
     * @return read p99 latency
     */
    public long readP99Micros() {
        return readP99Micros;
    }

    /**
     * Returns write p50 latency.
     *
     * @return write p50 latency
     */
    public long writeP50Micros() {
        return writeP50Micros;
    }

    /**
     * Returns write p95 latency.
     *
     * @return write p95 latency
     */
    public long writeP95Micros() {
        return writeP95Micros;
    }

    /**
     * Returns write p99 latency.
     *
     * @return write p99 latency
     */
    public long writeP99Micros() {
        return writeP99Micros;
    }
}
