package org.hestiastore.index.segmentindex.runtimemonitoring;

import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;

/**
 * Runtime monitoring API for one segment index.
 */
public interface IndexRuntimeMonitoring {

    /**
     * Returns immutable runtime metrics/state snapshot.
     *
     * @return runtime snapshot
     */
    IndexRuntimeSnapshot snapshot();
}
