package com.coroptis.index;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Allows to use {@link PairReader} as {@link Iterator}. Some operations like
 * data merging it makes a lot easier. It support optimistic locking of source
 * reader.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class PairIteratorReader<K, V> implements PairIterator<K, V> {

    private final PairReader<K, V> reader;

    private Pair<K, V> current = null;

    private final OptimisticLock lock;

    public PairIteratorReader(final PairReader<K, V> reader) {
        this(reader, null);
    }

    public PairIteratorReader(final PairReader<K, V> reader,
            final OptimisticLock optimisticLock) {
        this.reader = Objects.requireNonNull(reader,
                "Pair reader can't be null.");
        current = reader.read();
        this.lock = optimisticLock;
    }

    public Optional<Pair<K, V>> readCurrent() {
        return Optional.ofNullable(current);
    }

    @Override
    public boolean hasNext() {
        if (lock == null) {
            return current != null;
        } else {
            if (lock.isLocked()) {
                return false;
            } else {
                return current != null;
            }
        }
    }

    @Override
    public Pair<K, V> next() {
        if (current == null) {
            throw new NoSuchElementException();
        }
        final Pair<K, V> out = current;
        current = reader.read();
        return out;
    }

    @Override
    public void close() {
        reader.close();
    }

}
