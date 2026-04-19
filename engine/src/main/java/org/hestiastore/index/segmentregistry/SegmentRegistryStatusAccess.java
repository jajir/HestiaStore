package org.hestiastore.index.segmentregistry;

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

    SegmentRegistryResult<Segment<K, V>> tryLoadSegment(SegmentId segmentId);

    SegmentRegistryResult<SegmentId> allocateSegmentId();

    SegmentRegistryResult<Segment<K, V>> tryCreateSegment();

    SegmentRegistryResult<Void> tryDeleteSegment(SegmentId segmentId);
}
