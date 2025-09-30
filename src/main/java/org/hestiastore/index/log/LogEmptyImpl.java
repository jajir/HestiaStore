package org.hestiastore.index.log;

public class LogEmptyImpl<K, V> implements Log<K, V> {

    @Override
    public void rotate() {
        // Do nothing
    }

    @Override
    public void post(final K key, final V value) {
        // Do nothing̦
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
