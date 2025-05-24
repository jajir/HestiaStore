package com.hestiastore.index.cache;

import java.util.Optional;

/**
 * Provide simple (key,value) in memory cache. Cache support different eviction
 * algorithm.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface Cache<K, V> {

    /**
     * Add element into cache. When cache is full than some element coul'd be
     * removed.
     * 
     * @param key
     * @param value
     */
    void put(K key, V value);

    /**
     * Return value object mapped to key.
     * 
     * @param key required key object
     * @return return associated value object otherwise return empty.
     */
    Optional<V> get(K key);

    /**
     * Allows to mark some value object as invalid. When value is in cache than
     * is removed from cache.
     * 
     * @param key
     */
    void ivalidate(K key);

    /**
     * Allows to invalidate all cache elements.
     */
    void invalidateAll();

}
