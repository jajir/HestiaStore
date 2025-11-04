package org.hestiastore.index;

/**
 * Resource that store key value entry. Before freeing from memory it requires
 * close method should be called. Close method persists all changes.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface EntryWriter<K, V> extends CloseableResource {

    /**
     * Allows to insert key value entry somewhere.
     * 
     * @param entry required key value entry
     */
    void write(Entry<K, V> entry);

    /**
     * Allows to insert key value entry somewhere.
     * 
     * @param key   required key
     * @param value required value
     */
    default void write(final K key, final V value) {
        write(Entry.of(key, value));
    }

}
