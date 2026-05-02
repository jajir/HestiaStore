package org.hestiastore.index.segmentindex.metrics;

import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;

/**
 * Collects a read-only metrics snapshot for a segment index runtime.
 */
public interface RuntimeMetricsCollector {

    /**
     * Creates a builder for runtime metrics collectors.
     *
     * @param <M> key type
     * @param <N> value type
     * @return runtime metrics collector builder
     */
    static <M, N> RuntimeMetricsCollectorBuilder<M, N> builder() {
        return new RuntimeMetricsCollectorBuilder<>();
    }

    /**
     * Captures all currently available index runtime metrics.
     *
     * @return immutable metrics snapshot
     */
    SegmentIndexMetricsSnapshot metricsSnapshot();
}
