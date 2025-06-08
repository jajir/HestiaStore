package org.hestiastore.index;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Allows to use {@link CloseablePairReader} as {@link Iterator}. Some
 * operations like data merging it makes a lot easier. It support optimistic
 * locking of source reader.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class PairIteratorFromReader<K, V>
        implements PairIteratorWithCurrent<K, V> {

    private final CloseablePairReader<K, V> reader;

    private Pair<K, V> next;
    private Pair<K, V> current;

    public PairIteratorFromReader(final CloseablePairReader<K, V> reader) {
        this.reader = Objects.requireNonNull(reader,
                "Pair reader can't be null.");
        next = reader.read();
        current = null;
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        return Optional.ofNullable(current);
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Pair<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        current = next;
        next = reader.read();
        return current;
    }

    @Override
    public void close() {
        reader.close();
    }

}
