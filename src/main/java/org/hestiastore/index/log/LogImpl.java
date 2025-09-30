package org.hestiastore.index.log;

import org.hestiastore.index.Vldtn;

public class LogImpl<K, V> implements Log<K, V> {

    private final LogWriter<K, V> logWriter;

    public LogImpl(final LogWriter<K, V> logWriter) {
        this.logWriter = Vldtn.requireNonNull(logWriter, "logWriter");
    }

    @Override
    public void rotate() {
        logWriter.rotate();
    }

    @Override
    public void post(final K key, final V value) {
        logWriter.post(key, value);
    }

    @Override
    public void delete(final K key, final V value) {
        logWriter.delete(key, value);
    }

    @Override
    public void close() {
        logWriter.close();
    }

}
