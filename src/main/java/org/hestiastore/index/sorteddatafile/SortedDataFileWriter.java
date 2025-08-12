package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Pair;

//TODO consider renaming, SortedPairWriter.

/**
 * Abstraction for writing to sorted data file.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public interface SortedDataFileWriter<K, V> extends CloseableResource {

    void write(final Pair<K, V> pair);

    long writeFull(final Pair<K, V> pair);

}
