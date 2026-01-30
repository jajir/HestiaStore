package org.hestiastore.index.segmentindex;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentHandlerLockStatus;
import org.hestiastore.index.segmentregistry.SegmentRegistryFreeze;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;

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

    SegmentRegistryResult<SegmentRegistryFreeze> tryEnterFreeze();

    SegmentRegistryResult<Void> evictSegmentFromCache(SegmentId segmentId,
            Segment<K, V> expected);

    SegmentRegistryResult<Void> deleteSegmentFiles(SegmentId segmentId);

    void failRegistry();
}
