package org.hestiastore.index.log;

import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;

/**
 * Unsorted key value pairs log file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class LogUnsortedFileWriterImpl<K, V>
        implements LogUnsortedFileWriter<K, V> {

    private final PairWriter<LoggedKey<K>, V> writer;

    public LogUnsortedFileWriterImpl(final PairWriter<LoggedKey<K>, V> writer) {
        this.writer = Vldtn.requireNonNull(writer, "writer");
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
