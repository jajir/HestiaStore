package org.hestiastore.index.cache;

import java.util.Iterator;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;

public class UniqueCacheReader<K, V> implements CloseablePairReader<K, V> {

    private final Iterator<Pair<K, V>> iterator;

    UniqueCacheReader(final Iterator<Pair<K, V>> iterator) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
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
