package org.hestiastore.index;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class PairIteratorList<K, V> extends AbstractCloseableResource
        implements PairIteratorWithCurrent<K, V> {

    private final Iterator<Pair<K, V>> iterator;
    private Pair<K, V> currentPair = null;

    public PairIteratorList(final List<Pair<K, V>> list) {
        this(Vldtn.requireNonNull(list, "list").iterator());
    }

    public PairIteratorList(final Iterator<Pair<K, V>> iterator) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
    }

    @Override
    public boolean hasNext() {
        return !wasClosed() && iterator.hasNext();
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        if (wasClosed()) {
            throw new NoSuchElementException();
        }
        return Optional.ofNullable(currentPair);
    }

    @Override
    public Pair<K, V> next() {
        if (wasClosed()) {
            throw new NoSuchElementException();
        }
        currentPair = iterator.next();
        return currentPair;
    }

    @Override
    protected void doClose() {
        currentPair = null;
    }

}
