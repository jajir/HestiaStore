package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.cache.UniqueCache;

public class PairSupplierRefreshedFromCache<K, V>
        implements PairSupplier<K, V> {

    private final PairSupplier<K, V> pairSupplier;
    private final UniqueCache<K, V> cache;

    PairSupplierRefreshedFromCache(final PairSupplier<K, V> pairSupplier,
            final UniqueCache<K, V> cache) {
        this.pairSupplier = Objects.requireNonNull(pairSupplier);
        this.cache = Objects.requireNonNull(cache);
    }

    @Override
    public Pair<K, V> get() {
        final Pair<K, V> pair = pairSupplier.get();
        if (pair == null) {
            return null;
        }
        final V value = cache.get(pair.getKey());
        if (value == null) {
            return pair;
        }
        return Pair.of(pair.getKey(), value);
    }

}
