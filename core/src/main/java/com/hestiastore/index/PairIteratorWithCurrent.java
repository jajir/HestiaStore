package com.hestiastore.index;

import java.util.Optional;

/**
 * Define key value pair iterator. It allows to read current pair
 * {@link #getCurrent()}.
 * 
 * @param <K>
 * @param <V>
 */
public interface PairIteratorWithCurrent<K, V> extends PairIterator<K, V> {

    /**
     * Return current pair.
     * 
     * If there is no pair method return empty optional. It could happend in
     * case when iterator is empty or all pairs was read.
     * 
     * @return
     */
    public Optional<Pair<K, V>> getCurrent();

}
