package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;

/**
 * Abstraction for writing to sorted data file.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
// TODO fullWriteShould be implement in different place
public interface SortedDataFileFullWriter<K, V> extends PairWriter<K, V> {

    void write(final Pair<K, V> pair);

    long writeFull(final Pair<K, V> pair);

}
