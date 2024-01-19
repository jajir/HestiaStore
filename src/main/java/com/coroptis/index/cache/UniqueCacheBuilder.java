package com.coroptis.index.cache;

import java.util.Comparator;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.sstfile.SstFile;

/**
 * Class allows to instantiate unique cache and fill it with data.
 * 
 * @author honza
 *
 */
public class UniqueCacheBuilder<K, V> {

    private Comparator<K> keyComparator;

    private SstFile<K, V> sstFile;

    UniqueCacheBuilder() {

    }

    public UniqueCacheBuilder<K, V> withKeyComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = keyComparator;
        return this;
    }

    public UniqueCacheBuilder<K, V> withSstFile(SstFile<K, V> sstFile) {
        this.sstFile = sstFile;
        return this;
    }

    public UniqueCache<K, V> build() {
        final UniqueCache<K, V> out = new UniqueCache<>(keyComparator);
        try (final PairReader<K, V> fileStreamer = sstFile.openReader()) {
            Pair<K, V> pair = null;
            while ((pair = fileStreamer.read()) != null) {
                out.put(pair);
            }
        }
        return out;
    }

}
