package org.hestiastore.index;

/**
 * Resource that store key value pair. Before freeing from memory it requires
 * close method should be called. Close method persists all changes.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface PairWriter<K, V> extends CloseableResource {

    /**
     * Allows to insert key value pair somewhere.
     * 
     * @param pair required key value pair
     */
    void write(Pair<K, V> pair);

    /**
     * Allows to insert key value pair somewhere.
     * 
     * @param key   required key
     * @param value required value
     */
    default void write(final K key, final V value) {
        write(Pair.of(key, value));
    }

}
