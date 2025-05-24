package com.hestiastore.index;

/**
 * Resource that store key value pair. Before freeing from memory it requires
 * close method should be called. Close method persists all changes.
 * 
 * @author honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public interface PairWriter<K, V> extends CloseableResource {

    /**
     * Allows to insert key value pair somewhere.
     * 
     * @param pair required key value pair
     */
    void put(Pair<K, V> pair);

    default void put(final K key, final V value) {
        put(Pair.of(key, value));
    }

}
