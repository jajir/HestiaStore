package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairReader;

public class PairSupplierFromIterator<K, V> implements PairReader<K, V> {

    private final PairIterator<K, V> pairIterator;

    PairSupplierFromIterator(final PairIterator<K, V> pairIterator) {
        this.pairIterator = Objects.requireNonNull(pairIterator);
    }

    @Override
    public Pair<K, V> read() {
        if (pairIterator.hasNext()) {
            return pairIterator.next();
        } else {
            return null;
        }
    }

}
