package org.hestiastore.index.monitoring;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;

/**
 * Read-only monitoring view of one logical index.
 */
public interface MonitoredIndex {

    /**
     * Immutable runtime snapshot for the index.
     *
     * @return runtime snapshot
     */
    IndexRuntimeSnapshot runtimeSnapshot();

    /**
     * Logical index name.
     *
     * @return index name
     */
    default String indexName() {
        return runtimeSnapshot().indexName();
    }

    /**
     * Current lifecycle state of the index.
     *
     * @return lifecycle state
     */
    default SegmentIndexState state() {
        return runtimeSnapshot().state();
    }

    /**
     * Convenience readiness flag.
     *
     * @return true when index state is READY
     */
    default boolean ready() {
        return state() == SegmentIndexState.READY;
    }
}
