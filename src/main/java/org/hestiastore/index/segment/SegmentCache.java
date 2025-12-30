package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Combines an in-memory write cache with an existing delta cache and exposes a
 * merged view for lookups and iteration.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentCache<K, V> {

    private final UniqueCache<K, V> deltaCache;
    private final UniqueCache<K, V> writeCache;
    private final Comparator<K> keyComparator;
    private final TypeDescriptor<V> valueTypeDescriptor;

    public SegmentCache(final Comparator<K> keyComparator,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this(keyComparator, valueTypeDescriptor, null);
    }

    public SegmentCache(final Comparator<K> keyComparator,
            final TypeDescriptor<V> valueTypeDescriptor,
            final List<Entry<K, V>> deltaEntries) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.deltaCache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyComparator)
                .withThreadSafe(true)
                .buildEmpty();
        this.writeCache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyComparator)
                .withThreadSafe(true)
                .buildEmpty();
        if (deltaEntries != null) {
            for (final Entry<K, V> entry : deltaEntries) {
                putToDeltaCache(entry);
            }
        }
    }

    /**
     * Adds a new entry into the write cache.
     *
     * @param entry entry to cache
     */
    public void put(final Entry<K, V> entry) {
        writeCache.put(Vldtn.requireNonNull(entry, "entry"));
    }

    /**
     * Adds a new entry into the write cache.
     *
     * @param key required key
     * @param value value to store
     */
    public void put(final K key, final V value) {
        put(Entry.of(Vldtn.requireNonNull(key, "key"), value));
    }

    /**
     * Adds an entry into the delta cache portion.
     *
     * @param entry entry to store
     */
    public void putToDeltaCache(final Entry<K, V> entry) {
        deltaCache.put(Vldtn.requireNonNull(entry, "entry"));
    }

    /**
     * Adds an entry into the delta cache portion.
     *
     * @param key required key
     * @param value value to store
     */
    public void putToDeltaCache(final K key, final V value) {
        putToDeltaCache(Entry.of(Vldtn.requireNonNull(key, "key"), value));
    }

    /**
     * Returns the value from the write cache if present, otherwise falls back
     * to the delta cache.
     *
     * @param key required key
     * @return value or null if missing
     */
    public V get(final K key) {
        Vldtn.requireNonNull(key, "key");
        final V fromWrite = writeCache.get(key);
        if (fromWrite != null) {
            return fromWrite;
        }
        return deltaCache.get(key);
    }

    /**
     * Returns the total number of unique keys across both caches.
     *
     * @return size of merged view
     */
    public int size() {
        if (writeCache.isEmpty()) {
            return deltaCache.size();
        }
        if (deltaCache.isEmpty()) {
            return writeCache.size();
        }
        return buildMergedCache().size();
    }

    /**
     * Counts the number of entries excluding tombstones.
     *
     * @return number of non-tombstone entries
     */
    public int sizeWithoutTombstones() {
        int count = 0;
        for (final Entry<K, V> entry : getAsSortedList()) {
            if (!valueTypeDescriptor.isTombstone(entry.getValue())) {
                count++;
            }
        }
        return count;
    }

    /**
     * Clears both caches.
     */
    public void evictAll() {
        deltaCache.clear();
        writeCache.clear();
    }

    /**
     * Returns entries from the merged view as a sorted list.
     *
     * @return sorted list of entries
     */
    public List<Entry<K, V>> getAsSortedList() {
        if (writeCache.isEmpty() && deltaCache.isEmpty()) {
            return List.of();
        }
        return buildMergedCache().getAsSortedList();
    }

    List<Entry<K, V>> getWriteCacheAsSortedList() {
        if (writeCache.isEmpty()) {
            return List.of();
        }
        return writeCache.getAsSortedList();
    }

    void clearWriteCache() {
        writeCache.clear();
    }

    private UniqueCache<K, V> buildMergedCache() {
        final UniqueCache<K, V> merged = UniqueCache.<K, V>builder()
                .withKeyComparator(keyComparator)
                .buildEmpty();
        addAll(merged, deltaCache.getAsList());
        addAll(merged, writeCache.getAsList());
        return merged;
    }

    private void addAll(final UniqueCache<K, V> target,
            final List<Entry<K, V>> entries) {
        Vldtn.requireNonNull(target, "target");
        if (entries == null || entries.isEmpty()) {
            return;
        }
        for (final Entry<K, V> entry : entries) {
            if (entry == null) {
                continue;
            }
            target.put(entry);
        }
    }
}
