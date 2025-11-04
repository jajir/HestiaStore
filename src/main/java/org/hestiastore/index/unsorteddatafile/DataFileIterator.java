package org.hestiastore.index.unsorteddatafile;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

/**
 * An iterator over key-value entries stored in a data file.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class DataFileIterator<K, V> extends AbstractCloseableResource
        implements EntryIteratorWithCurrent<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReader reader;

    private Entry<K, V> current;
    private Entry<K, V> next;

    /**
     * Constructs a new {@code DataFileIterator} with the specified key and
     * value type readers and file reader.
     *
     * @param keyReader   required type reader for keys
     * @param valueReader required type reader for values
     * @param reader      required file reader to read from
     * @throws IllegalArgumentException if any of the arguments is null
     */
    public DataFileIterator(final TypeReader<K> keyReader,
            final TypeReader<V> valueReader, final FileReader reader) {
        this.keyTypeReader = Vldtn.requireNonNull(keyReader, "keyReader");
        this.valueTypeReader = Vldtn.requireNonNull(valueReader, "valueReader");
        this.reader = Vldtn.requireNonNull(reader, "reader");
        tryToReadNext();
    }

    /**
     * Indicates whether another key/value entry is available in the underlying
     * file. The method does not advance the iterator.
     *
     * @return {@code true} when a subsequent entry can be returned by
     *         {@link #next()}, otherwise {@code false}
     */
    @Override
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Returns the next key/value entry from the file and advances the iterator.
     *
     * @return the next {@link Entry}
     * @throws NoSuchElementException if the iterator has already been
     *                                exhausted
     */
    @Override
    public Entry<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException("No more elements");
        }
        current = next;
        tryToReadNext();
        return current;
    }

    private void tryToReadNext() {
        final K key = keyTypeReader.read(reader);
        if (key == null) {
            next = null;
        } else {
            final V value = valueTypeReader.read(reader);
            next = new Entry<K, V>(key, value);
        }
    }

    /**
     * Releases the underlying {@link FileReader}. It is safe to call multiple
     * times.
     */
    @Override
    protected void doClose() {
        reader.close();
    }

    /**
     * Returns the most recently returned entry, if any. The value is only
     * present after the first successful call to {@link #next()} and remains
     * unchanged until the iterator advances again.
     *
     * @return optional containing the current entry or empty when iteration has
     *         not started yet
     */
    @Override
    public Optional<Entry<K, V>> getCurrent() {
        return Optional.ofNullable(current);
    }

}
