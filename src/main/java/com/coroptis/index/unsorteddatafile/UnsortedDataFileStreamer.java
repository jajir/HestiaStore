package com.coroptis.index.unsorteddatafile;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;

public class UnsortedDataFileStreamer<K, V> implements CloseableResource {

    private final UnsortedDataFileSpliterator<K, V> spliterator;

    UnsortedDataFileStreamer(
            final UnsortedDataFileSpliterator<K, V> spliterator) {
        this.spliterator = spliterator;
    }

    public Stream<Pair<K, V>> stream() {
        if (spliterator == null) {
            return Stream.empty();
        }
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public void close() {
        if (spliterator == null) {
            return;
        }
        spliterator.close();
    }

}
