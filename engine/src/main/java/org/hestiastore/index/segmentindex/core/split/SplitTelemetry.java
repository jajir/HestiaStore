package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.metrics.Stats;

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
     * Adapts shared runtime stats to the split telemetry contract.
     *
     * @param stats stats collector
     * @return split telemetry adapter
     */
    static SplitTelemetry from(final Stats stats) {
        final Stats validatedStats = Vldtn.requireNonNull(stats, "stats");
        return new SplitTelemetry() {
            @Override
            public void recordSplitScheduled() {
                validatedStats.recordSplitScheduled();
            }

            @Override
            public void recordSplitTaskStartDelayNanos(final long nanos) {
                validatedStats.recordSplitTaskStartDelayNanos(nanos);
            }

            @Override
            public void recordSplitTaskRunLatencyNanos(final long nanos) {
                validatedStats.recordSplitTaskRunLatencyNanos(nanos);
            }
        };
    }

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
