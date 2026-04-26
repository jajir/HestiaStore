package org.hestiastore.index.segmentindex.core.metrics;

import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;

/**
 * Central read-only metrics service for a segment index runtime.
 */
public interface MetricService {

    /**
     * Creates a builder for metric services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return metric service builder
     */
    static <M, N> MetricServiceBuilder<M, N> builder() {
        return new MetricServiceBuilder<>();
    }

    /**
     * Captures all currently available index runtime metrics.
     *
     * @return immutable metrics snapshot
     */
    SegmentIndexMetricsSnapshot metricsSnapshot();
}
