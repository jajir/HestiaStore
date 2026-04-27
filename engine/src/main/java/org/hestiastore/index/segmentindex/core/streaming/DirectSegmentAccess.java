package org.hestiastore.index.segmentindex.core.streaming;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Opens routed window iterators for runtime streaming operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface DirectSegmentAccess<K, V> {

    static <K, V> DirectSegmentAccess<K, V> create(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        return new DirectSegmentCoordinator<>(keyToSegmentMap, segmentRegistry,
                retryPolicy);
    }

    EntryIterator<K, V> openWindowIterator(SegmentWindow resolvedWindows,
            SegmentIteratorIsolation isolation);
}
