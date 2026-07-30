package org.hestiastore.index.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

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
     * @param keyComparator   required comparator for keys
     * @param initialCapacity retained capacity hint; must not be negative
     */
    protected UniqueCache(final Comparator<K> keyComparator,
            final int initialCapacity) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        Vldtn.requireGreaterThanOrEqualToZero(initialCapacity,
                "initialCapacity");
        this.map = new ConcurrentHashMap<>(initialCapacity, 0.75f, 1);
    }

    Comparator<K> getKeyComparator() {
        return keyComparator;
    }

    /**
     * When there is old value than old value is rewritten.
     */
    public void put(final Entry<K, V> entry) {
        putAndReportNewKey(entry);
    }

    /**
     * Stores an entry and reports whether it added a previously absent key.
     *
     * @param entry entry to store
     * @return true when the key was not present before this write
     */
    public boolean putAndReportNewKey(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        final K key = Vldtn.requireNonNull(entry.getKey(), "entry.key");
        final V value = Vldtn.requireNonNull(entry.getValue(), "entry.value");
        final V previous = map.put(key, value);
        return previous == null;
    }

    /**
     * Stores an entry only when its key is not already present.
     *
     * @param entry entry to store
     * @return true when the entry was added
     */
    public boolean putIfAbsent(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        final K key = Vldtn.requireNonNull(entry.getKey(), "entry.key");
        final V value = Vldtn.requireNonNull(entry.getValue(), "entry.value");
        return map.putIfAbsent(key, value) == null;
    }

    /**
     * Replaces an entry only when its current value is the observed value.
     *
     * @param key key to replace
     * @param observedValue value previously read from this cache
     * @param newValue replacement value
     * @return true when the value was replaced
     */
    public boolean replace(final K key, final V observedValue,
            final V newValue) {
        return map.replace(Vldtn.requireNonNull(key, "key"),
                Vldtn.requireNonNull(observedValue, "observedValue"),
                Vldtn.requireNonNull(newValue, "newValue"));
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
        final List<Entry<K, V>> out = snapshotEntries();
        if (out.size() < 2) {
            return out;
        }
        out.sort(Comparator.comparing(Entry::getKey, keyComparator));
        return out;
    }

    /**
     * Get all entries as list.
     * 
     * @return list of entries
     */
    public List<Entry<K, V>> getAsList() {
        return snapshotEntries();
    }

    /**
     * Returns an iterator over a sorted shallow snapshot of keys. Concurrent
     * updates during snapshot creation may or may not be reflected. The
     * returned iterator does not support removal. In the 100,000/500,000-key
     * JMH comparison, this direct-array implementation allocated about 30%
     * less memory per operation than sorting
     * {@code new ArrayList<>(map.keySet())}.
     *
     * @return iterator over keys sorted by the configured comparator
     */
    public Iterator<K> getSortedKeyIterator() {
        if (map.isEmpty()) {
            return List.<K>of().iterator();
        }
        // The key-set array contains only K instances and remains internal.
        @SuppressWarnings("unchecked")
        final K[] keys = (K[]) map.keySet().toArray();
        if (keys.length > 1) {
            Arrays.sort(keys, keyComparator);
        }
        return Arrays.asList(keys).iterator();
    }

    /**
     * Iterates over cache entries as key/value pairs. Concurrent updates may or
     * may not be visible during traversal.
     *
     * @param consumer consumer for each key/value pair
     */
    public void forEachEntry(final BiConsumer<K, V> consumer) {
        Vldtn.requireNonNull(consumer, "consumer");
        if (map.isEmpty()) {
            return;
        }
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Returns a snapshot of entries and clears the cache.
     *
     * @return snapshot of entries at the time of clearing
     */
    public List<Entry<K, V>> snapshotAndClear() {
        final List<Entry<K, V>> snapshot = snapshotEntries();
        map.clear();
        return snapshot;
    }

    private List<Entry<K, V>> snapshotEntries() {
        if (map.isEmpty()) {
            return List.of();
        }
        final List<Entry<K, V>> out = new ArrayList<>(map.size());
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            out.add(new Entry<>(entry.getKey(), entry.getValue()));
        }
        return out;
    }
}
