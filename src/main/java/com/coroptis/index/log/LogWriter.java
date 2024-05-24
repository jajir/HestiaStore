package com.coroptis.index.log;

import com.coroptis.index.CloseableResource;

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
public interface LogWriter<K, V> extends CloseableResource {

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
     * @param key   required key
     * @param value required value, it should be thomstone value
     */
    void delete(K key, V value);

}
