package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Stable-segment runtime capability used by routed operations and maintenance.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface StableSegmentAccess<K, V> {

    static <K, V> StableSegmentAccess<K, V> create(
            final SegmentRegistry<K, V> segmentRegistry) {
        return new StableSegmentGateway<>(segmentRegistry);
    }

    IndexResult<EntryIterator<K, V>> openIterator(SegmentId segmentId,
            SegmentIteratorIsolation isolation);

    IndexResult<BlockingSegment<K, V>> compact(SegmentId segmentId);

    IndexResult<BlockingSegment<K, V>> flush(SegmentId segmentId);
}
