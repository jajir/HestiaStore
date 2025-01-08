package com.coroptis.index.sstfile;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIteratorWithCurrent;

public class PairIteratorWithCurrentComparator<K, V>
        implements Comparator<PairIteratorWithCurrent<K, V>> {

    private final Comparator<K> keyComparator;

    public PairIteratorWithCurrentComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = Objects.requireNonNull(keyComparator);
    }

    @Override
    public int compare(final PairIteratorWithCurrent<K, V> iter1,
            final PairIteratorWithCurrent<K, V> iter2) {

        if (iter1 == null && iter2 == null) {
            return 0;
        } else if (iter1 == null) {
            return -1;
        } else if (iter2 == null) {
            return 1;
        }

        final Optional<Pair<K, V>> oPair1 = iter1.getCurrent();
        final Optional<Pair<K, V>> oPair2 = iter2.getCurrent();

        if (oPair1.isPresent()) {
            if (oPair2.isPresent()) {
                final K k1 = oPair1.get().getKey();
                final K k2 = oPair2.get().getKey();
                return keyComparator.compare(k1, k2);
            } else {
                return 1;
            }
        } else {
            if (oPair2.isPresent()) {
                return -1;
            } else {
                return 0;
            }
        }

    }

}
