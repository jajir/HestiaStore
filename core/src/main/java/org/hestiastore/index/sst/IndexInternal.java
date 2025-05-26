package org.hestiastore.index.sst;

import java.util.stream.Stream;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;

public interface IndexInternal<K, V> extends Index<K, V> {

    PairIterator<K, V> openSegmentIterator(SegmentWindow segmentWindows);

    default Stream<Pair<K, V>> getStream(final SegmentWindow segmentWindow) {
        throw new UnsupportedOperationException(
                "should be definec in the concrete class");
    }

}
