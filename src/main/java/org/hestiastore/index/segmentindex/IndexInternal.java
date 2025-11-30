package org.hestiastore.index.segmentindex;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;

public interface IndexInternal<K, V> extends SegmentIndex<K, V> {

    EntryIterator<K, V> openSegmentIterator(SegmentWindow segmentWindows);

    default Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow) {
        throw new UnsupportedOperationException(
                "should be definec in the concrete class");
    }

}
