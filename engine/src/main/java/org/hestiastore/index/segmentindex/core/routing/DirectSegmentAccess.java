package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Routed direct-operation capability used by runtime services.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface DirectSegmentAccess<K, V> {

    static <K, V> DirectSegmentAccess<K, V> create(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentAccess<K, V> stableSegmentAccess,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final IndexRetryPolicy retryPolicy) {
        return new DirectSegmentCoordinator<>(keyToSegmentMap, segmentRegistry,
                stableSegmentAccess, backgroundSplitCoordinator, retryPolicy);
    }

    IndexResult<V> get(K key);

    EntryIterator<K, V> openWindowIterator(SegmentWindow resolvedWindows,
            SegmentIteratorIsolation isolation);

    IndexResult<Void> put(K key, V value);
}
