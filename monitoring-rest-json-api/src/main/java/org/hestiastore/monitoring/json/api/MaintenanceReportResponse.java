package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;
import java.util.Objects;

/**
 * Maintenance metrics section inside an index report payload.
 */
@SuppressWarnings("java:S6206")
public final class MaintenanceReportResponse {

    private final long compactRequestCount;
    private final long flushRequestCount;
    private final long flushAcceptedToReadyP95Micros;
    private final long compactAcceptedToReadyP95Micros;
    private final long flushBusyRetryCount;
    private final long compactBusyRetryCount;
    private final ExecutorReportResponse indexExecutor;
    private final ExecutorReportResponse stableSegmentExecutor;

    /**
     * Creates maintenance metrics.
     *
     * @param compactRequestCount compact request count
     * @param flushRequestCount flush request count
     * @param flushAcceptedToReadyP95Micros flush accepted-to-ready p95
     * @param compactAcceptedToReadyP95Micros compact accepted-to-ready p95
     * @param flushBusyRetryCount flush busy retry count
     * @param compactBusyRetryCount compact busy retry count
     * @param indexExecutor index maintenance executor metrics
     * @param stableSegmentExecutor stable segment executor metrics
     */
    @ConstructorProperties({ "compactRequestCount", "flushRequestCount",
            "flushAcceptedToReadyP95Micros",
            "compactAcceptedToReadyP95Micros", "flushBusyRetryCount",
            "compactBusyRetryCount", "indexExecutor",
            "stableSegmentExecutor" })
    @SuppressWarnings("java:S107")
    public MaintenanceReportResponse(final long compactRequestCount,
            final long flushRequestCount,
            final long flushAcceptedToReadyP95Micros,
            final long compactAcceptedToReadyP95Micros,
            final long flushBusyRetryCount, final long compactBusyRetryCount,
            final ExecutorReportResponse indexExecutor,
            final ExecutorReportResponse stableSegmentExecutor) {
        this.compactRequestCount = compactRequestCount;
        this.flushRequestCount = flushRequestCount;
        this.flushAcceptedToReadyP95Micros = flushAcceptedToReadyP95Micros;
        this.compactAcceptedToReadyP95Micros =
                compactAcceptedToReadyP95Micros;
        this.flushBusyRetryCount = flushBusyRetryCount;
        this.compactBusyRetryCount = compactBusyRetryCount;
        this.indexExecutor = Objects.requireNonNull(indexExecutor,
                "indexExecutor");
        this.stableSegmentExecutor = Objects.requireNonNull(
                stableSegmentExecutor, "stableSegmentExecutor");
    }

    /**
     * Returns compact request count.
     *
     * @return compact request count
     */
    public long compactRequestCount() {
        return compactRequestCount;
    }

    /**
     * Returns flush request count.
     *
     * @return flush request count
     */
    public long flushRequestCount() {
        return flushRequestCount;
    }

    /**
     * Returns flush accepted-to-ready p95.
     *
     * @return flush accepted-to-ready p95
     */
    public long flushAcceptedToReadyP95Micros() {
        return flushAcceptedToReadyP95Micros;
    }

    /**
     * Returns compact accepted-to-ready p95.
     *
     * @return compact accepted-to-ready p95
     */
    public long compactAcceptedToReadyP95Micros() {
        return compactAcceptedToReadyP95Micros;
    }

    /**
     * Returns flush busy retry count.
     *
     * @return flush busy retry count
     */
    public long flushBusyRetryCount() {
        return flushBusyRetryCount;
    }

    /**
     * Returns compact busy retry count.
     *
     * @return compact busy retry count
     */
    public long compactBusyRetryCount() {
        return compactBusyRetryCount;
    }

    /**
     * Returns index maintenance executor metrics.
     *
     * @return index maintenance executor metrics
     */
    public ExecutorReportResponse indexExecutor() {
        return indexExecutor;
    }

    /**
     * Returns stable segment executor metrics.
     *
     * @return stable segment executor metrics
     */
    public ExecutorReportResponse stableSegmentExecutor() {
        return stableSegmentExecutor;
    }
}
