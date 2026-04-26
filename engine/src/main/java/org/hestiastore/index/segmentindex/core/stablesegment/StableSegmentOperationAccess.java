package org.hestiastore.index.segmentindex.core.stablesegment;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Stable-segment runtime capability used by streaming and maintenance.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface StableSegmentOperationAccess<K, V> {

    static <K, V> StableSegmentOperationAccess<K, V> create(
            final SegmentRegistry<K, V> segmentRegistry) {
        return new StableSegmentOperationGateway<>(segmentRegistry);
    }

    StableSegmentOperationResult<EntryIterator<K, V>> openIterator(
            SegmentId segmentId, SegmentIteratorIsolation isolation);

    StableSegmentOperationResult<BlockingSegment<K, V>> compact(
            SegmentId segmentId);

    StableSegmentOperationResult<BlockingSegment<K, V>> flush(
            SegmentId segmentId);
}
