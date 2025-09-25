package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.PairIterator;

/**
 * Define functionality that allows to search through sorted data file.
 */
public interface SortedDataFileIteratorProvider<K, V> {

    /**
     * Allows to read from specific position.
     *
     * @param position the position in the file to start reading from
     * @return PairIterator that allows to read key-value pairs
     */
    PairIterator<K, V> openIteratorAtPosition(long position);

    /**
     * Read data from the beginning of the file.
     *
     * @return PairIterator that allows to read key-value pairs
     */
    PairIterator<K, V> openIterator();

}
