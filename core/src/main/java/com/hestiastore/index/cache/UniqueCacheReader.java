package com.hestiastore.index.cache;

import java.util.Iterator;
import java.util.Objects;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.Pair;

public class UniqueCacheReader<K, V> implements CloseablePairReader<K, V> {

    private final Iterator<Pair<K, V>> iterator;

    UniqueCacheReader(final Iterator<Pair<K, V>> iterator) {
        this.iterator = Objects.requireNonNull(iterator);
    }

    @Override
    public void close() {
        // do nothing, it's not possible to close iterator.
    }

    @Override
    public Pair<K, V> read() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

}
