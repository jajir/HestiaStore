package org.hestiastore.index.log;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;

/**
 * Unsorted key value entries log file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class LogUnsortedFileWriterImpl<K, V>
        extends AbstractCloseableResource implements LogUnsortedFileWriter<K, V> {

    private final EntryWriter<LoggedKey<K>, V> writer;

    public LogUnsortedFileWriterImpl(final EntryWriter<LoggedKey<K>, V> writer) {
        this.writer = Vldtn.requireNonNull(writer, "writer");
    }

    public void post(final K key, final V value) {
        writer.write(LoggedKey.of(LogOperation.POST, key), value);
    }

    public void delete(final K key, final V value) {
        writer.write(LoggedKey.of(LogOperation.DELETE, key), value);
    }

    @Override
    protected void doClose() {
        writer.close();
    }

}
