package org.hestiastore.index.cache;

import java.util.Comparator;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * Class allows to instantiate unique cache and fill it with data.
 * 
 * @author honza
 *
 */
public class UniqueCacheBuilder<K, V> {

    private Comparator<K> keyComparator;

    private SortedDataFile<K, V> sdf;

    protected UniqueCacheBuilder() {

    }

    public UniqueCacheBuilder<K, V> withKeyComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = keyComparator;
        return this;
    }

    public UniqueCacheBuilder<K, V> withDataFile(
            final SortedDataFile<K, V> dataFile) {
        this.sdf = dataFile;
        return this;
    }

    public UniqueCache<K, V> build() {
        final UniqueCache<K, V> out = new UniqueCache<>(keyComparator);
        try (EntryIterator<K, V> iterator = sdf.openIterator()) {
            Entry<K, V> entry = null;
            while (iterator.hasNext()) {
                entry = iterator.next();
                out.put(entry);
            }
        }
        return out;
    }

}
