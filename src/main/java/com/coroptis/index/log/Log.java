package com.coroptis.index.log;

import java.util.stream.Stream;

import com.coroptis.index.Pair;

/**
 * Write Ahead Log. Supporting basic operations:
 * <ul>
 * <li>POST - Create or update data record</li>
 * <li>DEKLETE - Delete data record</li>
 * </ul>
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Log<K, V> {

    /**
     * Log post operation.
     * 
     * @param key   required key
     * @param value required value
     */
    void post(K key, V value);

    /**
     * Log delete operation.
     * 
     * @param key required key
     */
    void delete(K key);

    /**
     * Provide stream over all data from older log record to lastest one.
     * 
     * @return stream containing all logged data
     */
    Stream<Pair<K, V>> getStream();

}
