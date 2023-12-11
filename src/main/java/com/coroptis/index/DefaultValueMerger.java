package com.coroptis.index;

/**
 * Default implementation suppose that key value pairs are always same.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class DefaultValueMerger<K, V> implements ValueMerger<K, V> {

    /**
     * Default implementation suppose that both values are same and return
     * second one.
     * 
     * When same key could have different value that it's necessary to provide
     * own implementation.
     */
    @Override
    public V merge(final K key, final V value1, final V value2) {
        return value2;
    }

}
