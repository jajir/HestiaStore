package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorStreamer;

/**
 * Abstraction for unsorted key-value storage backed by the {@code Directory}
 * API.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface UnsortedDataFile<K, V> {

    static <K, V> UnsortedDataFileBuilder<K, V> builder() {
        return new UnsortedDataFileBuilder<>();
    }

    PairIterator<K, V> openIterator();

    PairIteratorStreamer<K, V> openStreamer();

    UnsortedDataFileWriterTx<K, V> openWriterTx();
}
