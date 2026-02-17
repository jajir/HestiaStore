package org.hestiastore.index.segmentindex;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;

/**
 * Internal extension of {@link SegmentIndex} for segment-level operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface IndexInternal<K, V> extends SegmentIndex<K, V> {

    /**
     * Opens a segment iterator over the requested window.
     *
     * @param segmentWindows window selecting segments to iterate
     * @return iterator over the selected segments
     */
    EntryIterator<K, V> openSegmentIterator(SegmentWindow segmentWindows);

    /**
     * Streams entries over the requested window.
     *
     * @param segmentWindow window selecting segments to stream
     * @return stream of entries
     */
    default Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow) {
        throw new UnsupportedOperationException(
                "should be definec in the concrete class");
    }

}
