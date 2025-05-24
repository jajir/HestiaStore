package com.hestiastore.index.cache;

import java.util.Comparator;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.Pair;
import com.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * Class allows to instantiate unique cache and fill it with data.
 * 
 * @author honza
 *
 */
public class UniqueCacheBuilder<K, V> {

    private Comparator<K> keyComparator;

    private SortedDataFile<K, V> sdf;

    UniqueCacheBuilder() {

    }

    public UniqueCacheBuilder<K, V> withKeyComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = keyComparator;
        return this;
    }

    public UniqueCacheBuilder<K, V> withSstFile(
            final SortedDataFile<K, V> sstFile) {
        this.sdf = sstFile;
        return this;
    }

    public UniqueCache<K, V> build() {
        final UniqueCache<K, V> out = new UniqueCache<>(keyComparator);
        try (CloseablePairReader<K, V> pairReader = sdf.openReader()) {
            Pair<K, V> pair = null;
            while ((pair = pairReader.read()) != null) {
                out.put(pair);
            }
        }
        return out;
    }

}
