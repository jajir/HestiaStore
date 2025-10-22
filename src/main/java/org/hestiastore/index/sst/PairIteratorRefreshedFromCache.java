package org.hestiastore.index.sst;

import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;

public class PairIteratorRefreshedFromCache<K, V>
        extends AbstractCloseableResource implements PairIterator<K, V> {

    private final PairIterator<K, V> pairIterator;
    private final UniqueCache<K, V> cache;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private Pair<K, V> currentPair = null;

    PairIteratorRefreshedFromCache(final PairIterator<K, V> pairIterator,
            final UniqueCache<K, V> cache,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.pairIterator = Vldtn.requireNonNull(pairIterator, "pairIterator");
        this.cache = Vldtn.requireNonNull(cache, "cache");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
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
    protected void doClose() {
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
