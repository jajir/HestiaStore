package org.hestiastore.index.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicInteger size = new AtomicInteger();
    private final boolean threadSafe;

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
    protected UniqueCache(final Comparator<K> keyComparator,
            int initialCapacity) {
        this(keyComparator, initialCapacity, false);
    }

    protected UniqueCache(final Comparator<K> keyComparator,
            final int initialCapacity, final boolean threadSafe) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.map = threadSafe
                ? new ConcurrentHashMap<>(initialCapacity)
                : new HashMap<>(initialCapacity, 0.75F);
        this.threadSafe = threadSafe;
    }

    Comparator<K> getKeyComparator() {
        return keyComparator;
    }

    /**
     * When there is old value than old value is rewritten.
     */
    public void put(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        final K key = Vldtn.requireNonNull(entry.getKey(), "entry.key");
        final V value = Vldtn.requireNonNull(entry.getValue(), "entry.value");
        final V previous = map.put(key, value);
        if (previous == null) {
            size.incrementAndGet();
        }
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
        size.set(0);
    }

    /**
     * Get number of key value entries in cache.
     * 
     * @return number of key value entries in cache
     */
    public int size() {
        return size.get();
    }

    /**
     * Is cache empty?
     * 
     * @return true when cache is empty
     */
    public boolean isEmpty() {
        return size.get() == 0;
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
     * Returns an iterator over a sorted snapshot of keys.
     *
     * @return iterator over keys sorted by the configured comparator
     */
    public Iterator<K> getSortedKeyIterator() {
        if (map.isEmpty()) {
            return List.<K>of().iterator();
        }
        final List<K> keys = new ArrayList<>(map.keySet());
        if (keys.size() > 1) {
            keys.sort(keyComparator);
        }
        return keys.iterator();
    }

    /**
     * Iterates over a snapshot of the cache entries as key/value pairs.
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
        size.set(0);
        return snapshot;
    }

    private List<Entry<K, V>> snapshotEntries() {
        return snapshotEntriesLocked();
    }

    private List<Entry<K, V>> snapshotEntriesLocked() {
        if (map.isEmpty()) {
            return List.of();
        }
        final List<Entry<K, V>> out = new ArrayList<>(map.size());
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            out.add(new Entry<>(entry.getKey(), entry.getValue()));
        }
        return out;
    }

    boolean isThreadSafe() {
        return threadSafe;
    }
}
