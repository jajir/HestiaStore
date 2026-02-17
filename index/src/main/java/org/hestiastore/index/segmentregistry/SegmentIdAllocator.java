package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.segment.SegmentId;

/**
 * Allocates new segment identifiers.
 */
@FunctionalInterface
public interface SegmentIdAllocator {

    /**
     * Returns the next available segment id.
     *
     * @return next segment id
     */
    SegmentId nextId();
}
