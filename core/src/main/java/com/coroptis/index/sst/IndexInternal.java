package com.coroptis.index.sst;

import java.util.stream.Stream;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

public interface IndexInternal<K, V> extends Index<K, V> {

    PairIterator<K, V> openSegmentIterator(SegmentWindow segmentWindows);

    default public Stream<Pair<K, V>> getStream(
            final SegmentWindow segmentWindow) {
        throw new UnsupportedOperationException(
                "should be definec in the concrete class");
    }

}
