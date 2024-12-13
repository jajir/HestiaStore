package com.coroptis.index.log;

import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class LogEmptyImpl<K, V> implements Log<K, V> {

    @Override
    public LogWriter<K, V> openWriter() {
        return new LogWriter<K, V>() {

            @Override
            public void post(K key, V value) {
                // Intentionally do nothing
            }

            @Override
            public void delete(K key, V value) {
                // Intentionally do nothing
            }

            @Override
            public void close() {
                // Intentionally do nothing
            }
        };
    }

    @Override
    public UnsortedDataFileStreamer<LoggedKey<K>, V> openStreamer() {
        return new UnsortedDataFileStreamer<LoggedKey<K>, V>(null);
    }

    @Override
    public void rotate() {
        throw new UnsupportedOperationException(
                "Unimplemented method 'rotate'");
    }

}
