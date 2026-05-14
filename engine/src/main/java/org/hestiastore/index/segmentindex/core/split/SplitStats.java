package org.hestiastore.index.segmentindex.core.split;

/**
 * Immutable split runtime statistics exposed to metrics collection.
 */
public final class SplitStats {

    private final long splitScheduleCount;
    private final int splitInFlightCount;
    private final int splitBlockedCount;
    private final long splitTaskStartDelayP95Micros;
    private final long splitTaskRunLatencyP95Micros;

    /**
     * Creates a split stats snapshot.
     *
     * @param splitScheduleCount number of split tasks accepted for scheduling
     * @param splitInFlightCount number of scheduled or running split tasks
     * @param splitBlockedCount number of blocked segments with active split
     *        work
     * @param splitTaskStartDelayP95Micros p95 split task start delay
     * @param splitTaskRunLatencyP95Micros p95 split task runtime
     */
    public SplitStats(final long splitScheduleCount,
            final int splitInFlightCount, final int splitBlockedCount,
            final long splitTaskStartDelayP95Micros,
            final long splitTaskRunLatencyP95Micros) {
        this.splitScheduleCount = Math.max(0L, splitScheduleCount);
        this.splitInFlightCount = Math.max(0, splitInFlightCount);
        this.splitBlockedCount = Math.max(0, splitBlockedCount);
        this.splitTaskStartDelayP95Micros = Math.max(0L,
                splitTaskStartDelayP95Micros);
        this.splitTaskRunLatencyP95Micros = Math.max(0L,
                splitTaskRunLatencyP95Micros);
    }

    /**
     * @return number of split tasks accepted for scheduling
     */
    public long splitScheduleCount() {
        return splitScheduleCount;
    }

    /**
     * @return number of scheduled or running split tasks
     */
    public int splitInFlightCount() {
        return splitInFlightCount;
    }

    /**
     * @return number of blocked segments with active split work
     */
    public int splitBlockedCount() {
        return splitBlockedCount;
    }

    /**
     * @return p95 split task start delay in microseconds
     */
    public long splitTaskStartDelayP95Micros() {
        return splitTaskStartDelayP95Micros;
    }

    /**
     * @return p95 split task runtime in microseconds
     */
    public long splitTaskRunLatencyP95Micros() {
        return splitTaskRunLatencyP95Micros;
    }
}
