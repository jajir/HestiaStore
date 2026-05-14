package org.hestiastore.index.segmentindex.core.split;

/**
 * Records split telemetry without exposing metrics implementation details to
 * split orchestration code.
 */
interface SplitTelemetry {

    /**
     * Records that a split candidate was scheduled.
     */
    void recordSplitScheduled();

    /**
     * Records scheduler-to-start latency for a split task.
     *
     * @param nanos latency in nanoseconds
     */
    void recordSplitTaskStartDelayNanos(long nanos);

    /**
     * Records runtime latency for a split task.
     *
     * @param nanos runtime in nanoseconds
     */
    void recordSplitTaskRunLatencyNanos(long nanos);

    /**
     * @return no-op telemetry for tests that do not care about metrics
     */
    static SplitTelemetry noOp() {
        return new SplitTelemetry() {
            @Override
            public void recordSplitScheduled() {
                // No telemetry sink is attached for this no-op adapter.
            }

            @Override
            public void recordSplitTaskStartDelayNanos(final long nanos) {
                // No telemetry sink is attached for this no-op adapter.
            }

            @Override
            public void recordSplitTaskRunLatencyNanos(final long nanos) {
                // No telemetry sink is attached for this no-op adapter.
            }
        };
    }
}
