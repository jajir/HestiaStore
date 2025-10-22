package org.hestiastore.index.sorteddatafile;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;

/**
 * An empty pair iterator with current that always indicates no next element and
 * has no current element.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
public class EmptyPairIteratorWithCurrent<K, V>
        extends AbstractCloseableResource implements PairIteratorWithCurrent<K, V> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Pair<K, V> next() {
        throw new NoSuchElementException();
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        return Optional.empty();
    }

    @Override
    protected void doClose() {
        // nothing to release
    }

}
