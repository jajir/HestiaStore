package org.hestiastore.index.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    private static final Lock NOOP_LOCK = new NoopLock();

    private final Map<K, V> map;

    private final Comparator<K> keyComparator;
    private final Lock readLock;
    private final Lock writeLock;

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
        this(keyComparator, initialCapacity, NOOP_LOCK, NOOP_LOCK);
    }

    protected UniqueCache(final Comparator<K> keyComparator, int initialCapacity,
            final Lock readLock, final Lock writeLock) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.map = new HashMap<>(initialCapacity, 0.75F);
        this.readLock = Vldtn.requireNonNull(readLock, "readLock");
        this.writeLock = Vldtn.requireNonNull(writeLock, "writeLock");
    }

    Comparator<K> getKeyComparator() {
        return keyComparator;
    }

    /**
     * When there is old value than old value is rewritten.
     */
    public void put(final Entry<K, V> entry) {
        writeLock.lock();
        try {
            map.put(entry.getKey(), entry.getValue());
        } finally {
            writeLock.unlock();
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
        readLock.lock();
        try {
            return map.get(key);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Clear all data in cache.
     */
    public void clear() {
        writeLock.lock();
        try {
            map.clear();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Get number of key value entries in cache.
     * 
     * @return number of key value entries in cache
     */
    public int size() {
        readLock.lock();
        try {
            return map.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Is cache empty?
     * 
     * @return true when cache is empty
     */
    public boolean isEmpty() {
        readLock.lock();
        try {
            return map.isEmpty();
        } finally {
            readLock.unlock();
        }
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
     * Returns a snapshot of entries and clears the cache.
     *
     * @return snapshot of entries at the time of clearing
     */
    public List<Entry<K, V>> snapshotAndClear() {
        writeLock.lock();
        try {
            final List<Entry<K, V>> snapshot = snapshotEntriesLocked();
            map.clear();
            return snapshot;
        } finally {
            writeLock.unlock();
        }
    }

    private List<Entry<K, V>> snapshotEntries() {
        readLock.lock();
        try {
            return snapshotEntriesLocked();
        } finally {
            readLock.unlock();
        }
    }

    private List<Entry<K, V>> snapshotEntriesLocked() {
        if (map.isEmpty()) {
            return List.of();
        }
        final List<Entry<K, V>> out = new ArrayList<>(map.size());
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            if (entry == null || entry.getKey() == null) {
                continue;
            }
            out.add(new Entry<>(entry.getKey(), entry.getValue()));
        }
        return out;
    }

    private static final class NoopLock extends ReentrantLock {

        @Override
        public void lock() {
            // no-op
        }

        @Override
        public void unlock() {
            // no-op
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public void lockInterruptibly() {
            // no-op
        }

        @Override
        public boolean tryLock(final long time,
                final java.util.concurrent.TimeUnit unit) {
            return true;
        }
    }
}
