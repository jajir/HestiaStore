package org.hestiastore.index.unsorteddatafile;

import java.util.Optional;

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
public class DataFileIterator<K, V> implements PairIteratorWithCurrent<K, V> {

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
        next();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Pair<K, V> next() {
        current = next;
        final K key = keyTypeReader.read(reader);
        if (key == null) {
            next = null;
        } else {
            final V value = valueTypeReader.read(reader);
            next = new Pair<K, V>(key, value);
        }
        return current;
    }

    @Override
    public void close() {
        reader.close();
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        return Optional.ofNullable(current);
    }

}
