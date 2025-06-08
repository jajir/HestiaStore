package org.hestiastore.index;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class PairIteratorList<K, V> implements PairIteratorWithCurrent<K, V> {

    private final Iterator<Pair<K, V>> iterator;
    private Pair<K, V> currentPair = null;
    private boolean closed = false;

    public PairIteratorList(final List<Pair<K, V>> list) {
        this(Vldtn.requireNonNull(list, "list").iterator());
    }

    public PairIteratorList(final Iterator<Pair<K, V>> iterator) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
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
