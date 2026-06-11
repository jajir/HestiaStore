package org.hestiastore.index.segmentindex.runtimemonitoring;

import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;

/**
 * Runtime monitoring API for one segment index.
 */
public interface IndexRuntimeMonitoring {

    /**
     * Creates a builder for runtime monitoring views.
     *
     * @param <M> key type
     * @param <N> value type
     * @return runtime monitoring builder
     */
    static <M, N> IndexRuntimeMonitoringBuilder<M, N> builder() {
        return new IndexRuntimeMonitoringBuilder<>();
    }

    /**
     * Returns immutable runtime metrics/state snapshot.
     *
     * @return runtime snapshot
     */
    IndexRuntimeSnapshot snapshot();
}
