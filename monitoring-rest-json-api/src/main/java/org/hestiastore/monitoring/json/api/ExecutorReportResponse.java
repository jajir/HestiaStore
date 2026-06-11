package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Executor metrics section inside an index report payload.
 */
public final class ExecutorReportResponse {

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
     * @param queueSize current queue size
     * @param queueCapacity configured queue capacity
     * @param completedTaskCount completed task count
     * @param rejectedTaskCount rejected task count
     * @param callerRunsCount caller-runs count
     */
    @ConstructorProperties({ "activeThreadCount", "queueSize",
            "queueCapacity", "completedTaskCount", "rejectedTaskCount",
            "callerRunsCount" })
    public ExecutorReportResponse(final int activeThreadCount,
            final int queueSize, final int queueCapacity,
            final long completedTaskCount, final long rejectedTaskCount,
            final long callerRunsCount) {
        this.activeThreadCount = activeThreadCount;
        this.queueSize = queueSize;
        this.queueCapacity = queueCapacity;
        this.completedTaskCount = completedTaskCount;
        this.rejectedTaskCount = rejectedTaskCount;
        this.callerRunsCount = callerRunsCount;
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
     * Returns current queue size.
     *
     * @return current queue size
     */
    public int queueSize() {
        return queueSize;
    }

    /**
     * Returns configured queue capacity.
     *
     * @return configured queue capacity
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
