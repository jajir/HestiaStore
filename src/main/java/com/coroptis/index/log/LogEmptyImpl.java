package com.coroptis.index.log;

import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class LogEmptyImpl<K, V> implements Log<K, V> {

    @Override
    public UnsortedDataFileStreamer<LoggedKey<K>, V> openStreamer() {
        return new UnsortedDataFileStreamer<LoggedKey<K>, V>(null);
    }

    @Override
    public void rotate() {
        // Do nothing
    }

    @Override
    public void post(final K key, final V value) {
        // Do nothing
    }

    @Override
    public void delete(final K key, final V value) {
        // Do nothing
    }

    @Override
    public void close() {
        // Do nothing
    }

}
