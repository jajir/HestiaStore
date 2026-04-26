package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Capability view exposing only the runtime services needed by data-path
 * facade code.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndexDataAccess<K, V> {

    void put(K key, V value);

    V get(K key);

    void delete(K key);

    EntryIterator<K, V> openSegmentIterator(SegmentId segmentId,
            SegmentIteratorIsolation isolation);

    EntryIterator<K, V> openWindowIterator(SegmentWindow segmentWindow,
            SegmentIteratorIsolation isolation);
}
