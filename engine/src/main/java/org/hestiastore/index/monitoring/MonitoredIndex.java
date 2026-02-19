package org.hestiastore.index.monitoring;

import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Read-only monitoring view of one logical index.
 */
public interface MonitoredIndex {

    /**
     * Logical index name.
     *
     * @return index name
     */
    String indexName();

    /**
     * Current lifecycle state of the index.
     *
     * @return lifecycle state
     */
    SegmentIndexState state();

    /**
     * Immutable metrics snapshot for the index.
     *
     * @return metrics snapshot
     */
    SegmentIndexMetricsSnapshot metricsSnapshot();

    /**
     * Convenience readiness flag.
     *
     * @return true when index state is READY
     */
    default boolean ready() {
        return state() == SegmentIndexState.READY;
    }
}
