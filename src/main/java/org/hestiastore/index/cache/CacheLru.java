package org.hestiastore.index.cache;

/**
 * LRU cache abstraction with support for null markers and access tracking.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface CacheLru<K, V> extends Cache<K, V> {

    /**
     * Inserts a null marker to avoid repeated lookups for missing values.
     *
     * @param key key to store the null marker for
     */
    void putNull(K key);

}
