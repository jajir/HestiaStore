package org.hestiastore.index;

import java.util.Optional;

/**
 * Define key value entry iterator. It allows to read current entry
 * {@link #getCurrent()}.
 * 
 * @param <K>
 * @param <V>
 */
public interface EntryIteratorWithCurrent<K, V> extends EntryIterator<K, V> {

    /**
     * Return current entry.
     * 
 * If there is no entry method return empty optional. It could happen in
 * case when iterator is empty or all entries were read.
     * 
     * @return
     */
    Optional<Entry<K, V>> getCurrent();

}
