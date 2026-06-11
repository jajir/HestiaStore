package org.hestiastore.index.segmentindex.runtimemonitoring.model;

/**
 * User-facing metrics for one runtime executor.
 */
public final class SegmentIndexExecutorMetrics {

    private final int activeThreadCount;
    private final int queueSize;
    private final int queueCapacity;
    private final long completedTaskCount;
    private final long rejectedTaskCount;
    private final long callerRunsCount;

    /**
     * Creates executor metrics.
     *
     * @param activeThreadCount active thread count
     * @param queueSize queue size
     * @param queueCapacity queue capacity
     * @param completedTaskCount completed task count
     * @param rejectedTaskCount rejected task count
     * @param callerRunsCount caller-runs count
     */
    public SegmentIndexExecutorMetrics(final int activeThreadCount,
            final int queueSize, final int queueCapacity,
            final long completedTaskCount, final long rejectedTaskCount,
            final long callerRunsCount) {
        this.activeThreadCount = MetricModelValidation.nonNegative(
                activeThreadCount, "activeThreadCount");
        this.queueSize = MetricModelValidation.nonNegative(queueSize,
                "queueSize");
        this.queueCapacity = MetricModelValidation.nonNegative(queueCapacity,
                "queueCapacity");
        this.completedTaskCount = MetricModelValidation.nonNegative(
                completedTaskCount, "completedTaskCount");
        this.rejectedTaskCount = MetricModelValidation.nonNegative(
                rejectedTaskCount, "rejectedTaskCount");
        this.callerRunsCount = MetricModelValidation.nonNegative(
                callerRunsCount, "callerRunsCount");
    }

    /**
     * Returns active thread count.
     *
     * @return active thread count
     */
    public int activeThreadCount() {
        return activeThreadCount;
    }

    /**
     * Returns queue size.
     *
     * @return queue size
     */
    public int queueSize() {
        return queueSize;
    }

    /**
     * Returns queue capacity.
     *
     * @return queue capacity
     */
    public int queueCapacity() {
        return queueCapacity;
    }

    /**
     * Returns completed task count.
     *
     * @return completed task count
     */
    public long completedTaskCount() {
        return completedTaskCount;
    }

    /**
     * Returns rejected task count.
     *
     * @return rejected task count
     */
    public long rejectedTaskCount() {
        return rejectedTaskCount;
    }

    /**
     * Returns caller-runs count.
     *
     * @return caller-runs count
     */
    public long callerRunsCount() {
        return callerRunsCount;
    }
}
