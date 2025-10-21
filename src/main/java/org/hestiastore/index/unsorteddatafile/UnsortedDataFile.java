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

    /**
     * Creates a builder preconfigured for constructing {@link UnsortedDataFile}
     * instances.
     *
     * @param <K> key type
     * @param <V> value type
     * @return new builder instance
     */
    static <K, V> UnsortedDataFileBuilder<K, V> builder() {
        return new UnsortedDataFileBuilder<>();
    }

    /**
     * Opens a point-in-time iterator over the file contents. The caller is
     * responsible for closing the returned iterator.
     *
     * @return iterator providing access to all stored pairs
     */
    PairIterator<K, V> openIterator();

    /**
     * Opens a streaming view backed by an iterator. When the file does not
     * exist, an empty stream is returned.
     *
     * @return streaming adapter over the file contents
     */
    PairIteratorStreamer<K, V> openStreamer();

    /**
     * Opens a transactional writer that buffers data into a temporary file and
     * promotes it on commit.
     *
     * @return transactional writer handle
     */
    UnsortedDataFileWriterTx<K, V> openWriterTx();
}
