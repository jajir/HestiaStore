package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Read-only view of the current segment-index lifecycle state.
 */
public interface SegmentIndexStateView {

    /**
     * Returns the current segment-index lifecycle state.
     *
     * @return current segment-index lifecycle state
     */
    SegmentIndexState currentState();
}
