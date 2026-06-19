package org.hestiastore.index.segmentindex.core;

/**
 * Mutable runtime state boundary used by durability code to fail the owning
 * index after unrecoverable runtime errors.
 */
public interface SegmentIndexRuntimeState extends SegmentIndexStateView {

    /**
     * Marks the owning index as failed.
     *
     * @param failure runtime failure cause
     */
    void markRuntimeFailure(RuntimeException failure);
}
