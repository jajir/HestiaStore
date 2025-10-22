package org.hestiastore.index.unsorteddatafile;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

/**
 * An iterator over key-value pairs stored in a data file.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class DataFileIterator<K, V> extends AbstractCloseableResource
        implements PairIteratorWithCurrent<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReader reader;

    private Pair<K, V> current;
    private Pair<K, V> next;

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
     * Indicates whether another key/value pair is available in the underlying
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
     * Returns the next key/value pair from the file and advances the iterator.
     *
     * @return the next {@link Pair}
     * @throws NoSuchElementException if the iterator has already been
     *                                exhausted
     */
    @Override
    public Pair<K, V> next() {
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
            next = new Pair<K, V>(key, value);
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
     * Returns the most recently returned pair, if any. The value is only
     * present after the first successful call to {@link #next()} and remains
     * unchanged until the iterator advances again.
     *
     * @return optional containing the current pair or empty when iteration has
     *         not started yet
     */
    @Override
    public Optional<Pair<K, V>> getCurrent() {
        return Optional.ofNullable(current);
    }

}
