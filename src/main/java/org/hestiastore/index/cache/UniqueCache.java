package org.hestiastore.index.cache;

import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.sorteddatafile.EntryComparator;

/**
 * Cache for index operation. When there are two operations with same key value
 * than just latest is stored. Because just last one is valid.
 * 
 * This cache doesn't support eviction. When is full that all data are evicted
 * at once.
 */
public class UniqueCache<K, V> {

    private final Comparator<K> keyComparator;
    private final TreeMap<K, V> map;

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
    public UniqueCache(final Comparator<K> keyComparator) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.map = new TreeMap<>(keyComparator);
    }

    /**
     * When there is old value than old value is rewritten.
     */
    public void put(final Entry<K, V> entry) {
        map.merge(entry.getKey(), entry.getValue(), (oldVal, newVal) -> newVal);
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
     * It's unsorted.
     * 
     * @return
     */
    public List<Entry<K, V>> toList() {
        return map.entrySet().stream()
                .map(entry -> new Entry<K, V>(entry.getKey(), entry.getValue()))
                .toList();
    }

    public List<Entry<K, V>> getAsSortedList() {
        return map.entrySet().stream()
                .map(entry -> new Entry<K, V>(entry.getKey(), entry.getValue()))
                .sorted(new EntryComparator<>(keyComparator))//
                .toList();
    }

    public List<K> getSortedKeys() {
        return map.entrySet().stream()//
                .map(entry -> entry.getKey())//
                .sorted(keyComparator)//
                .toList();
    }

    public EntryIterator<K, V> getSortedIterator() {
        return EntryIterator.make(getAsSortedList().iterator());
    }

    /**
     * Get unsorted stream of key value entries
     * 
     * @return unsorted stream of key value entries
     */
    public Stream<Entry<K, V>> getStream() {
        return map.entrySet().stream()
                .map(entry -> new Entry<K, V>(entry.getKey(), entry.getValue()));
    }

}
