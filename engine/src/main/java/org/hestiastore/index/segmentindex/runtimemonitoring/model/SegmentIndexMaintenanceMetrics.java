package org.hestiastore.index.segmentindex.runtimemonitoring.model;

import org.hestiastore.index.Vldtn;

/**
 * User-facing maintenance metrics.
 */
public final class SegmentIndexMaintenanceMetrics {

    private final long compactRequestCount;
    private final long flushRequestCount;
    private final long flushAcceptedToReadyP95Micros;
    private final long compactAcceptedToReadyP95Micros;
    private final long flushBusyRetryCount;
    private final long compactBusyRetryCount;
    private final SegmentIndexExecutorMetrics indexExecutor;
    private final SegmentIndexExecutorMetrics stableSegmentExecutor;

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
     * @param stableSegmentExecutor stable segment maintenance executor metrics
     */
    @SuppressWarnings("java:S107")
    public SegmentIndexMaintenanceMetrics(final long compactRequestCount,
            final long flushRequestCount,
            final long flushAcceptedToReadyP95Micros,
            final long compactAcceptedToReadyP95Micros,
            final long flushBusyRetryCount,
            final long compactBusyRetryCount,
            final SegmentIndexExecutorMetrics indexExecutor,
            final SegmentIndexExecutorMetrics stableSegmentExecutor) {
        this.compactRequestCount = MetricModelValidation.nonNegative(
                compactRequestCount, "compactRequestCount");
        this.flushRequestCount = MetricModelValidation.nonNegative(
                flushRequestCount, "flushRequestCount");
        this.flushAcceptedToReadyP95Micros =
                MetricModelValidation.nonNegative(
                        flushAcceptedToReadyP95Micros,
                        "flushAcceptedToReadyP95Micros");
        this.compactAcceptedToReadyP95Micros =
                MetricModelValidation.nonNegative(
                        compactAcceptedToReadyP95Micros,
                        "compactAcceptedToReadyP95Micros");
        this.flushBusyRetryCount = MetricModelValidation.nonNegative(
                flushBusyRetryCount, "flushBusyRetryCount");
        this.compactBusyRetryCount = MetricModelValidation.nonNegative(
                compactBusyRetryCount, "compactBusyRetryCount");
        this.indexExecutor = Vldtn.requireNonNull(indexExecutor,
                "indexExecutor");
        this.stableSegmentExecutor = Vldtn.requireNonNull(
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
     * Returns flush accepted-to-ready p95 in microseconds.
     *
     * @return flush accepted-to-ready p95
     */
    public long flushAcceptedToReadyP95Micros() {
        return flushAcceptedToReadyP95Micros;
    }

    /**
     * Returns compact accepted-to-ready p95 in microseconds.
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
    public SegmentIndexExecutorMetrics indexExecutor() {
        return indexExecutor;
    }

    /**
     * Returns stable segment maintenance executor metrics.
     *
     * @return stable segment maintenance executor metrics
     */
    public SegmentIndexExecutorMetrics stableSegmentExecutor() {
        return stableSegmentExecutor;
    }
}
