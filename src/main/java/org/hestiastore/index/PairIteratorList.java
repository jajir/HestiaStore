package org.hestiastore.index;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public class PairIteratorList<K, V> implements PairIteratorWithCurrent<K, V> {

    private final Iterator<Pair<K, V>> iterator;
    private Pair<K, V> currentPair = null;
    private boolean closed = false;

    public PairIteratorList(final List<Pair<K, V>> list) {
        this(Objects.requireNonNull(list, "List can't be null.").iterator());
    }

    public PairIteratorList(final Iterator<Pair<K, V>> Iterator) {
        this.iterator = Objects.requireNonNull(Iterator,
                "Iterator can't be null.");
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext() && !closed;
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        if (closed) {
            throw new NoSuchElementException();
        }
        return Optional.ofNullable(currentPair);
    }

    @Override
    public Pair<K, V> next() {
        if (closed) {
            throw new NoSuchElementException();
        }
        currentPair = iterator.next();
        return currentPair;
    }

    @Override
    public void close() {
        closed = true;
        currentPair = null;
    }

}
