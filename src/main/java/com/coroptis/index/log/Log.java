package com.coroptis.index.log;

import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public interface Log<K, V> {

    static <M, N> LogBuilder<M, N> builder() {
        return new LogBuilder<M, N>();
    }

    /**
     * Provide stream over all data from older log record to lastest one.
     * 
     * @return
     */
    UnsortedDataFileStreamer<LoggedKey<K>, V> openStreamer();

    /**
     * Allows to write to log.
     * 
     * @return
     */
    LogWriter<K, V> openWriter();

    /**
     * 
     */
    void rotate();
}
