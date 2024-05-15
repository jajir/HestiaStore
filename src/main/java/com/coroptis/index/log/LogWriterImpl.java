package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.PairWriter;

/**
 * Unsorted key value pairs log file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LogWriterImpl<K, V> implements LogWriter<K, V> {

    private final PairWriter<LoggedKey<K>, V> writer;

    public static <M, N> LogBuilder<M, N> builder() {
        return new LogBuilder<M, N>();
    }

    public LogWriterImpl(final PairWriter<LoggedKey<K>, V> writer) {
        this.writer = Objects.requireNonNull(writer);
    }

    public void post(final K key, final V value) {
        writer.put(LoggedKey.of(LogOperation.POST, key), value);
    }

    public void delete(final K key, final V value) {
        writer.put(LoggedKey.of(LogOperation.DELETE, key), value);
    }

    @Override
    public void close() {
        writer.close();
    }

}
