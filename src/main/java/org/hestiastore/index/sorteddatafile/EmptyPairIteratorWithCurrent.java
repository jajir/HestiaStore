package org.hestiastore.index.sorteddatafile;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;

public class EmptyPairIteratorWithCurrent<K, V>
        implements PairIteratorWithCurrent<K, V> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Pair<K, V> next() {
        throw new NoSuchElementException();
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        return Optional.empty();
    }

}
