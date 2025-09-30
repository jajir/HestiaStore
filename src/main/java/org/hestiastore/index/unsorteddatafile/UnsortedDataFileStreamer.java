package org.hestiastore.index.unsorteddatafile;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;

//TODO this make it generic
public class UnsortedDataFileStreamer<K, V> implements CloseableResource {

    private final PairIterator<K, V> iterator;

    public UnsortedDataFileStreamer(final PairIterator<K, V> iterator) {
        this.iterator = iterator;
    }

    public Stream<Pair<K, V>> stream() {
        if (iterator == null) {
            return Stream.empty();
        }
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    @Override
    public void close() {
        if (iterator == null) {
            return;
        }
        iterator.close();
    }

}
