package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;
import java.util.Objects;

/**
 * Split metrics section inside an index report payload.
 */
@SuppressWarnings("java:S6206")
public final class SplitReportResponse {

    private final long scheduleCount;
    private final int inFlightCount;
    private final int blockedCount;
    private final long taskStartDelayP95Micros;
    private final long taskRunLatencyP95Micros;
    private final ExecutorReportResponse executor;

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
    @ConstructorProperties({ "scheduleCount", "inFlightCount", "blockedCount",
            "taskStartDelayP95Micros", "taskRunLatencyP95Micros",
            "executor" })
    public SplitReportResponse(final long scheduleCount,
            final int inFlightCount, final int blockedCount,
            final long taskStartDelayP95Micros,
            final long taskRunLatencyP95Micros,
            final ExecutorReportResponse executor) {
        this.scheduleCount = scheduleCount;
        this.inFlightCount = inFlightCount;
        this.blockedCount = blockedCount;
        this.taskStartDelayP95Micros = taskStartDelayP95Micros;
        this.taskRunLatencyP95Micros = taskRunLatencyP95Micros;
        this.executor = Objects.requireNonNull(executor, "executor");
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
     * Returns task start delay p95.
     *
     * @return task start delay p95
     */
    public long taskStartDelayP95Micros() {
        return taskStartDelayP95Micros;
    }

    /**
     * Returns task run latency p95.
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
    public ExecutorReportResponse executor() {
        return executor;
    }
}
