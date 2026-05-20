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
abstract class SegmentRegistryStatusAccess<K, V> {

    abstract OperationResult<Segment<K, V>> tryLoadSegment(SegmentId segmentId);

    abstract OperationResult<SegmentId> allocateSegmentId();

    abstract OperationResult<Segment<K, V>> tryCreateSegment();

    abstract OperationResult<Void> tryDeleteSegment(SegmentId segmentId);

    abstract OperationResult<Void> tryDeleteRetiredSegment(SegmentId segmentId);
}
