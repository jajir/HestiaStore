package org.hestiastore.index.segmentindex.core.split;

/**
 * Metrics-facing split runtime view.
 */
public interface SplitMetricsView {

    /**
     * Returns the current immutable split runtime metrics snapshot.
     *
     * @return split runtime metrics snapshot
     */
    SplitMetricsSnapshot metricsSnapshot();
}
