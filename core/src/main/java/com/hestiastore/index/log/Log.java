package com.hestiastore.index.log;

import com.hestiastore.index.unsorteddatafile.UnsortedDataFileStreamer;

public interface Log<K, V> {

    static <M, N> LogBuilder<M, N> builder() {
        return new LogBuilder<M, N>();
    }

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

    /**
     * Provide stream over all data from older log record to lastest one.
     * 
     * @return
     */
    UnsortedDataFileStreamer<LoggedKey<K>, V> openStreamer();

    /**
     * 
     */
    void rotate();

    /**
     * 
     */
    void close();
}
