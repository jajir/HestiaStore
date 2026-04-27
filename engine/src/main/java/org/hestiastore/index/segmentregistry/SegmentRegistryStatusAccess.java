package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Internal status-based protocol used by registry adapters and lifecycle
 * orchestration.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface SegmentRegistryStatusAccess<K, V> {

    OperationResult<Segment<K, V>> tryLoadSegment(SegmentId segmentId);

    OperationResult<SegmentId> allocateSegmentId();

    OperationResult<Segment<K, V>> tryCreateSegment();

    OperationResult<Void> tryDeleteSegment(SegmentId segmentId);
}
