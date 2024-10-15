package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;

public class PairSupplierRefreshedFromCache<K, V>
        implements PairSupplier<K, V> {

    private final PairSupplier<K, V> pairSupplier;
    private final UniqueCache<K, V> cache;
    private final TypeDescriptor<V> valueTypeDescriptor;

    PairSupplierRefreshedFromCache(final PairSupplier<K, V> pairSupplier,
            final UniqueCache<K, V> cache, final TypeDescriptor<V> valueTypeDescriptor) {
        this.pairSupplier = Objects.requireNonNull(pairSupplier);
        this.cache = Objects.requireNonNull(cache);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
    }

    @Override
    public Pair<K, V> get() {
        boolean readNextOne = true;
        while(true){
            final Pair<K, V> pair = pairSupplier.get();
            if (pair == null) {
                return null;
            }
            final V value = cache.get(pair.getKey());
            if (value == null) {
                return pair;
            }
            if(valueTypeDescriptor.isTombstone(value)){
                //nextRound
            }else{
                return Pair.of(pair.getKey(), value);
            }
        }
    }

}
