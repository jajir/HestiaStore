package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Internal extension of {@link SegmentIndex} for segment-level operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface IndexInternal<K, V> extends SegmentIndex<K, V> {

    /**
     * Opens a segment iterator over the requested window.
     *
     * @param segmentWindows window selecting segments to iterate
     * @return iterator over the selected segments
     */
    EntryIterator<K, V> openSegmentIterator(SegmentWindow segmentWindows);
}
