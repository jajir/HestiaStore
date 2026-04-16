package org.hestiastore.index.segmentindex.split;

/**
 * Metrics sink for background split task timings.
 */
public interface BackgroundSplitMetrics {

    BackgroundSplitMetrics NO_OP = new BackgroundSplitMetrics() {
        @Override
        public void recordSplitTaskStartDelayNanos(final long nanos) {
        }

        @Override
        public void recordSplitTaskRunLatencyNanos(final long nanos) {
        }
    };

    /**
     * Records time between task scheduling and task start.
     *
     * @param nanos delay in nanoseconds
     */
    void recordSplitTaskStartDelayNanos(long nanos);

    /**
     * Records wall clock task runtime.
     *
     * @param nanos runtime in nanoseconds
     */
    void recordSplitTaskRunLatencyNanos(long nanos);
}
