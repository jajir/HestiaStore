package org.hestiastore.index.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Cache for index operation. When there are two operations with same key value
 * than just latest is stored. Because just last one is valid.
 * 
 * This cache doesn't support eviction. When is full that all data are evicted
 * at once.
 */
public class UniqueCache<K, V> {

    private final Map<K, V> map;

    private final Comparator<K> keyComparator;

    /**
     * Create builder for unique cache.
     * 
     * @param <M> key type
     * @param <N> value type
     * @return builder
     */
    public static <M, N> UniqueCacheBuilder<M, N> builder() {
        return new UniqueCacheBuilder<>();
    }

    /**
     * Create unique cache with given key comparator.
     * 
     * @param keyComparator required comparator for keys
     */
    public UniqueCache(final Comparator<K> keyComparator, int initialCapacity) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.map = new HashMap<>(initialCapacity, 0.75F);
    }

    /**
     * When there is old value than old value is rewritten.
     */
    public void put(final Entry<K, V> entry) {
        map.put(entry.getKey(), entry.getValue());
    }

    /**
     * Get value for given key or null when there is no such key.
     * 
     * @param key required key
     * @return value or null
     */
    public V get(final K key) {
        Vldtn.requireNonNull(key, "key");
        return map.get(key);
    }

    /**
     * Clear all data in cache.
     */
    public void clear() {
        map.clear();
    }

    /**
     * Get number of key value entries in cache.
     * 
     * @return number of key value entries in cache
     */
    public int size() {
        return map.size();
    }

    /**
     * Is cache empty?
     * 
     * @return true when cache is empty
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Get all entries as sorted list.
     * 
     * Ih have to be sorted, because returned list is used for merging with
     * other data sources.
     * 
     * @return sorted list of entries
     */
    public List<Entry<K, V>> getAsSortedList() {
        @SuppressWarnings("unchecked")
        Map.Entry<K, V>[] a = map.entrySet().stream()
                .filter(e -> e != null && e.getKey() != null)
                .toArray(Map.Entry[]::new);
        final int n = a.length;
        if (n == 0) {
            return List.of();
        }

        /**
         * Sort array of map entries by key using the provided key comparator.
         * From performance point view it's best option for larger arrays.
         */
        Arrays.parallelSort(a, Map.Entry.comparingByKey(keyComparator));

        var out = new ArrayList<Entry<K, V>>(n);
        for (int i = 0; i < n; i++) {
            var e = a[i];
            out.add(new Entry<>(e.getKey(), e.getValue()));
        }
        return out;
    }

    /**
     * Get all entries as list.
     * 
     * @return list of entries
     */
    public List<Entry<K, V>> getAsList() {
        return map.entrySet().stream()//
                .map(entry -> new Entry<K, V>(entry.getKey(), entry.getValue()))
                .toList();
    }

}
