package org.hestiastore.index.segmentindex.runtimemonitoring;

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
