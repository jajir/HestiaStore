package com.coroptis.index.sst;

import java.util.NoSuchElementException;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;

public class PairIteratorRefreshedFromCache<K, V>
        implements PairIterator<K, V> {

    private final PairIterator<K, V> pairIterator;
    private final UniqueCache<K, V> cache;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private Pair<K, V> currentPair = null;

    PairIteratorRefreshedFromCache(final PairIterator<K, V> pairIterator,
            final UniqueCache<K, V> cache,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.pairIterator = Objects.requireNonNull(pairIterator);
        this.cache = Objects.requireNonNull(cache);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        currentPair = readNext();
    }

    @Override
    public boolean hasNext() {
        return currentPair != null;
    }

    @Override
    public Pair<K, V> next() {
        if (currentPair == null) {
            throw new NoSuchElementException("No more elements");
        }
        final Pair<K, V> pair = currentPair;
        currentPair = readNext();
        return pair;
    }

    @Override
    public void close() {
        pairIterator.close();
    }

    private Pair<K, V> readNext() {
        while (true) {
            if (!pairIterator.hasNext()) {
                return null;
            }
            final Pair<K, V> pair = pairIterator.next();
            final V value = cache.get(pair.getKey());
            if (value == null) {
                return pair;
            }
            if (!valueTypeDescriptor.isTombstone(value)) {
                return Pair.of(pair.getKey(), value);
            }
        }
    }

}
