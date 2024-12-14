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
public class LogUnsortedFileWriterImpl<K, V> implements LogUnsortedFileWriter<K, V> {

    private final PairWriter<LoggedKey<K>, V> writer;

    public LogUnsortedFileWriterImpl(final PairWriter<LoggedKey<K>, V> writer) {
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
