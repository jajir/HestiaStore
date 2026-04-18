package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildResult;
import org.hestiastore.index.segment.SegmentId;

/**
 * Internal contract for building a segment instance for the given id.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface SegmentBuildService<K, V> {

    /**
     * Builds a segment instance for the provided id.
     *
     * @param segmentId segment id
     * @return build result with status and optional segment
     */
    SegmentBuildResult<Segment<K, V>> buildSegment(SegmentId segmentId);
}
