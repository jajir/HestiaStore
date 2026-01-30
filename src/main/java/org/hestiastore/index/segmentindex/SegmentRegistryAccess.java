package org.hestiastore.index.segmentindex;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentHandlerLockStatus;

/**
 * Internal access for registry operations needed by split/maintenance flows.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface SegmentRegistryAccess<K, V> {

    boolean isSegmentInstance(SegmentId segmentId, Segment<K, V> expected);

    SegmentHandlerLockStatus lockSegmentHandler(SegmentId segmentId,
            Segment<K, V> expected);

    void unlockSegmentHandler(SegmentId segmentId, Segment<K, V> expected);
}
