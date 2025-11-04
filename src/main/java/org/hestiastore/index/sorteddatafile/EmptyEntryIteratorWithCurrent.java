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

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Entry<K, V> next() {
        throw new NoSuchElementException();
    }

    @Override
    public Optional<Entry<K, V>> getCurrent() {
        return Optional.empty();
    }

    @Override
    protected void doClose() {
        // nothing to release
    }

}
