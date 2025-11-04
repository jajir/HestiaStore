package org.hestiastore.index;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class EntryIteratorList<K, V> extends AbstractCloseableResource
        implements EntryIteratorWithCurrent<K, V> {

    private final Iterator<Entry<K, V>> iterator;
    private Entry<K, V> currentEntry = null;

    public EntryIteratorList(final List<Entry<K, V>> list) {
        this(Vldtn.requireNonNull(list, "list").iterator());
    }

    public EntryIteratorList(final Iterator<Entry<K, V>> iterator) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
    }

    @Override
    public boolean hasNext() {
        return !wasClosed() && iterator.hasNext();
    }

    @Override
    public Optional<Entry<K, V>> getCurrent() {
        if (wasClosed()) {
            throw new NoSuchElementException();
        }
        return Optional.ofNullable(currentEntry);
    }

    @Override
    public Entry<K, V> next() {
        if (wasClosed()) {
            throw new NoSuchElementException();
        }
        currentEntry = iterator.next();
        return currentEntry;
    }

    @Override
    protected void doClose() {
        currentEntry = null;
    }

}
