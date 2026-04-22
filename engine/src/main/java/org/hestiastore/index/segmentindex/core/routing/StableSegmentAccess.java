package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Stable-segment runtime capability used by routed operations and maintenance.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface StableSegmentAccess<K, V> {

    static <K, V> StableSegmentAccess<K, V> create(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry) {
        return new StableSegmentGateway<>(keyToSegmentMap, segmentRegistry);
    }

    OperationResult<V> get(K key);

    OperationResult<V> get(K key, SegmentId segmentId, long expectedTopologyVersion);

    OperationResult<V> get(SegmentId segmentId, K key);

    OperationResult<Void> put(SegmentId segmentId, K key, V value);

    OperationResult<EntryIterator<K, V>> openIterator(SegmentId segmentId,
            SegmentIteratorIsolation isolation);

    OperationResult<SegmentHandle<K, V>> compact(SegmentId segmentId);

    OperationResult<SegmentHandle<K, V>> flush(SegmentId segmentId);
}
