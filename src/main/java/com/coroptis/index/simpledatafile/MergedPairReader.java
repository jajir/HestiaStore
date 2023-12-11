package com.coroptis.index.simpledatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.ValueMerger;

public class MergedPairReader<K, V> implements PairReader<K, V> {

    private final PairReaderIterator<K, V> iterator1;
    private final PairReaderIterator<K, V> iterator2;
    private final Comparator<K> keyComparator;
    private final ValueMerger<K, V> valueMerger;

    public MergedPairReader(final PairReader<K, V> reader1,
            final PairReader<K, V> reader2, final ValueMerger<K, V> valueMerger,
            final Comparator<K> keyComparator) {
        this.iterator1 = new PairReaderIterator<>(reader1);
        this.iterator2 = new PairReaderIterator<>(reader2);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.valueMerger = Objects.requireNonNull(valueMerger);
    }

    @Override
    public void close() {
        iterator1.close();
        iterator2.close();
    }

    @Override
    public Pair<K, V> read() {
        if (iterator1.hasNext()) {
            if (iterator2.hasNext()) {
                final Pair<K, V> p1 = iterator1.readCurrent().get();
                final Pair<K, V> p2 = iterator2.readCurrent().get();
                final K k1 = p1.getKey();
                final K k2 = p2.getKey();
                final int cmp = keyComparator.compare(k1, k2);
                if (cmp == 0) {
                    // p1 == p2
                    iterator1.next();
                    iterator2.next();
                    return valueMerger.merge(p1, p2);
                } else if (cmp < 0) {
                    // p1 < p2
                    iterator1.next();
                    return p1;
                } else {
                    // p1 > p2
                    iterator2.next();
                    return p2;
                }
            } else {
                return iterator1.next();
            }
        } else {
            if (iterator2.hasNext()) {
                return iterator2.next();
            } else {
                return null;
            }
        }
    }

}
