package org.hestiastore.index.segmentindex.monitoring.model;

import org.hestiastore.index.Vldtn;

/**
 * User-facing split metrics.
 */
public final class SegmentIndexSplitMetrics {

    private final long scheduleCount;
    private final int inFlightCount;
    private final int blockedCount;
    private final long taskStartDelayP95Micros;
    private final long taskRunLatencyP95Micros;
    private final SegmentIndexExecutorMetrics executor;

    /**
     * Creates split metrics.
     *
     * @param scheduleCount split schedule count
     * @param inFlightCount split in-flight count
     * @param blockedCount blocked split candidate count
     * @param taskStartDelayP95Micros task start delay p95
     * @param taskRunLatencyP95Micros task run latency p95
     * @param executor split executor metrics
     */
    public SegmentIndexSplitMetrics(final long scheduleCount,
            final int inFlightCount, final int blockedCount,
            final long taskStartDelayP95Micros,
            final long taskRunLatencyP95Micros,
            final SegmentIndexExecutorMetrics executor) {
        this.scheduleCount = MetricModelValidation.nonNegative(scheduleCount,
                "scheduleCount");
        this.inFlightCount = MetricModelValidation.nonNegative(inFlightCount,
                "inFlightCount");
        this.blockedCount = MetricModelValidation.nonNegative(blockedCount,
                "blockedCount");
        this.taskStartDelayP95Micros = MetricModelValidation.nonNegative(
                taskStartDelayP95Micros, "taskStartDelayP95Micros");
        this.taskRunLatencyP95Micros = MetricModelValidation.nonNegative(
                taskRunLatencyP95Micros, "taskRunLatencyP95Micros");
        this.executor = Vldtn.requireNonNull(executor, "executor");
    }

    /**
     * Returns split schedule count.
     *
     * @return split schedule count
     */
    public long scheduleCount() {
        return scheduleCount;
    }

    /**
     * Returns split in-flight count.
     *
     * @return split in-flight count
     */
    public int inFlightCount() {
        return inFlightCount;
    }

    /**
     * Returns blocked split candidate count.
     *
     * @return blocked split candidate count
     */
    public int blockedCount() {
        return blockedCount;
    }

    /**
     * Returns task start delay p95 in microseconds.
     *
     * @return task start delay p95
     */
    public long taskStartDelayP95Micros() {
        return taskStartDelayP95Micros;
    }

    /**
     * Returns task run latency p95 in microseconds.
     *
     * @return task run latency p95
     */
    public long taskRunLatencyP95Micros() {
        return taskRunLatencyP95Micros;
    }

    /**
     * Returns split executor metrics.
     *
     * @return split executor metrics
     */
    public SegmentIndexExecutorMetrics executor() {
        return executor;
    }
}
