package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;

public class PairSupplierRefreshedFromCache<K, V>
        implements PairReader<K, V> {

    private final PairReader<K, V> pairReader;
    private final UniqueCache<K, V> cache;
    private final TypeDescriptor<V> valueTypeDescriptor;

    PairSupplierRefreshedFromCache(final PairReader<K, V> pairReader,
            final UniqueCache<K, V> cache, final TypeDescriptor<V> valueTypeDescriptor) {
        this.pairReader = Objects.requireNonNull(pairReader);
        this.cache = Objects.requireNonNull(cache);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
    }

    @Override
    public Pair<K, V> read() {
        while(true){
            final Pair<K, V> pair = pairReader.read();
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
