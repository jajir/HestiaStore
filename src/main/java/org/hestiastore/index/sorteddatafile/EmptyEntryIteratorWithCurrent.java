package org.hestiastore.index.sorteddatafile;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;

/**
 * An empty entry iterator with current that always indicates no next element and
 * has no current element.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
public class EmptyEntryIteratorWithCurrent<K, V>
        extends AbstractCloseableResource implements EntryIteratorWithCurrent<K, V> {

    /**
     * Always returns false because this iterator is empty.
     *
     * @return false
     */
    @Override
    public boolean hasNext() {
        return false;
    }

    /**
     * Always throws because this iterator is empty.
     *
     * @return never returns normally
     * @throws NoSuchElementException always
     */
    @Override
    public Entry<K, V> next() {
        throw new NoSuchElementException();
    }

    /**
     * Returns an empty current entry.
     *
     * @return empty optional
     */
    @Override
    public Optional<Entry<K, V>> getCurrent() {
        return Optional.empty();
    }

    /**
     * No-op close for an empty iterator.
     */
    @Override
    protected void doClose() {
        // nothing to release
    }

}
