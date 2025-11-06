package org.hestiastore.index.cache;

import java.util.Comparator;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
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

    /**
     * Optional initial capacity hint for the underlying map. If not provided or
     * set to a non-positive value, the default UniqueCache constructor is used.
     */
    private int initialCapacity = 0;

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

    /**
     * Provide initial capacity hint for the cache backing map. This can improve
     * performance when the expected number of unique keys is known upfront.
     *
     * @param initialCapacity number of entries the cache should size for
     * @return this builder
     * @throws IllegalArgumentException when initialCapacity is not greater than
     *                                  0
     */
    public UniqueCacheBuilder<K, V> withInitialCapacity(
            final int initialCapacity) {
        this.initialCapacity = Vldtn.requireGreaterThanZero(initialCapacity,
                "initialCapacity");
        return this;
    }

    public UniqueCache<K, V> build() {
        final UniqueCache<K, V> out = new UniqueCache<>(keyComparator,
                initialCapacity);
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
