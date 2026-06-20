package org.hestiastore.index.segmentindex.monitoring;

import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;

/**
 * Runtime monitoring API for one segment index.
 */
public interface SegmentIndexRuntimeMonitoring {

    /**
     * Returns immutable runtime metrics/state snapshot.
     *
     * @return runtime snapshot
     */
    SegmentIndexRuntimeSnapshot snapshot();
}
