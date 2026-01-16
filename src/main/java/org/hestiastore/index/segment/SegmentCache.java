package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
final class SegmentCache<K, V> {

    private final UniqueCache<K, V> deltaCache;
    private UniqueCache<K, V> writeCache;
    private UniqueCache<K, V> frozenWriteCache;
    private final Comparator<K> keyComparator;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInSegmentWriteCacheDuringFlush;
    private static final int MAX_INITIAL_CAPACITY = 1_000_000;
    private final ReentrantLock capacityLock = new ReentrantLock();
    private final Condition capacityAvailable = capacityLock.newCondition();

    /**
     * Creates a segment cache with initial delta entries and sizing limits.
     *
     * @param keyComparator comparator for ordering keys
     * @param valueTypeDescriptor descriptor for values (tombstone handling)
     * @param deltaEntries initial delta-cache entries, may be null
     * @param maxNumberOfKeysInSegmentWriteCache max write-cache size
     * @param maxNumberOfKeysInSegmentWriteCacheDuringFlush write-cache size
     *        allowed during flush
     * @param maxNumberOfKeysInSegmentCache max delta-cache size hint
     */
    public SegmentCache(final Comparator<K> keyComparator,
            final TypeDescriptor<V> valueTypeDescriptor,
            final List<Entry<K, V>> deltaEntries,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInSegmentWriteCacheDuringFlush,
            final int maxNumberOfKeysInSegmentCache) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.maxNumberOfKeysInSegmentWriteCache = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        this.maxNumberOfKeysInSegmentWriteCacheDuringFlush = Vldtn
                .requireGreaterThanZero(
                        maxNumberOfKeysInSegmentWriteCacheDuringFlush,
                        "maxNumberOfKeysInSegmentWriteCacheDuringFlush");
        final int deltaCapacityHint = Math.min(
                Vldtn.requireGreaterThanZero(maxNumberOfKeysInSegmentCache,
                        "maxNumberOfKeysInSegmentCache"),
                MAX_INITIAL_CAPACITY);
        this.deltaCache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyComparator)
                .withInitialCapacity(deltaCapacityHint).withThreadSafe(true)
                .buildEmpty();
        this.writeCache = buildWriteCache();
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
    public void putToWriteCache(final Entry<K, V> entry) {
        awaitCapacity();
        writeCache.put(Vldtn.requireNonNull(entry, "entry"));
    }

    /**
     * Attempts to add an entry to the write cache without blocking.
     *
     * @param entry entry to cache
     * @return true when the entry was accepted
     */
    boolean tryPutToWriteCacheWithoutWaiting(final Entry<K, V> entry) {
        capacityLock.lock();
        try {
            if (currentBufferedKeys() >= maxNumberOfKeysInSegmentWriteCache) {
                return false;
            }
            writeCache.put(Vldtn.requireNonNull(entry, "entry"));
            return true;
        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Adds an entry into the delta cache portion.
     *
     * @param entry entry to store
     */
    void putToDeltaCache(final Entry<K, V> entry) {
        deltaCache.put(Vldtn.requireNonNull(entry, "entry"));
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
        if (frozenWriteCache != null) {
            final V fromFrozen = frozenWriteCache.get(key);
            if (fromFrozen != null) {
                return fromFrozen;
            }
        }
        return deltaCache.get(key);
    }

    /**
     * Returns the total number of unique keys across both caches.
     *
     * @return size of merged view
     */
    public int size() {
        if (writeCache.isEmpty()
                && (frozenWriteCache == null || frozenWriteCache.isEmpty())) {
            return deltaCache.size();
        }
        if (deltaCache.isEmpty()
                && (frozenWriteCache == null || frozenWriteCache.isEmpty())) {
            return writeCache.size();
        }
        if (deltaCache.isEmpty() && writeCache.isEmpty()
                && frozenWriteCache != null) {
            return frozenWriteCache.size();
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
        if (frozenWriteCache != null) {
            frozenWriteCache.clear();
            frozenWriteCache = null;
        }
        signalCapacityAvailable();
    }

    /**
     * Clears the delta cache while preserving the current write cache.
     */
    void clearDeltaCachePreservingWriteCache() {
        deltaCache.clear();
        if (frozenWriteCache != null) {
            frozenWriteCache.clear();
            frozenWriteCache = null;
        }
        signalCapacityAvailable();
    }

    /**
     * Returns entries from the merged view as a sorted list.
     *
     * @return sorted list of entries
     */
    public List<Entry<K, V>> getAsSortedList() {
        if (writeCache.isEmpty() && deltaCache.isEmpty()
                && (frozenWriteCache == null || frozenWriteCache.isEmpty())) {
            return List.of();
        }
        return buildMergedCache().getAsSortedList();
    }

    /**
     * Freezes the current write cache into a snapshot for flushing.
     *
     * @return sorted snapshot entries, possibly empty
     */
    List<Entry<K, V>> freezeWriteCache() {
        if (frozenWriteCache != null && !frozenWriteCache.isEmpty()) {
            return frozenWriteCache.getAsSortedList();
        }
        if (writeCache.isEmpty()) {
            return List.of();
        }
        frozenWriteCache = writeCache;
        writeCache = buildWriteCache();
        return frozenWriteCache.getAsSortedList();
    }

    /**
     * Returns whether a frozen write cache snapshot is available.
     *
     * @return true when a frozen snapshot exists and is not empty
     */
    boolean hasFrozenWriteCache() {
        return frozenWriteCache != null && !frozenWriteCache.isEmpty();
    }

    /**
     * Merges the frozen write cache into the delta cache and clears the
     * snapshot.
     */
    void mergeFrozenWriteCacheToDeltaCache() {
        if (frozenWriteCache == null || frozenWriteCache.isEmpty()) {
            frozenWriteCache = null;
            signalCapacityAvailable();
            return;
        }
        addAll(deltaCache, frozenWriteCache.getAsList());
        frozenWriteCache.clear();
        frozenWriteCache = null;
        signalCapacityAvailable();
    }

    /**
     * Returns the number of unique keys in the write cache only.
     *
     * @return number of keys buffered for flush
     */
    int getNumberOfKeysInWriteCache() {
        return writeCache.size();
    }

    /**
     * Returns the total number of keys across delta, write, and frozen caches.
     *
     * @return total number of cached keys
     */
    int getNumbberOfKeysInCache() {
        final int frozen = frozenWriteCache == null ? 0
                : frozenWriteCache.size();
        return deltaCache.size() + writeCache.size() + frozen;
    }

    /**
     * Returns the number of currently buffered write keys, including frozen.
     *
     * @return total buffered write keys
     */
    private int currentBufferedKeys() {
        final int frozen = frozenWriteCache == null ? 0
                : frozenWriteCache.size();
        return writeCache.size() + frozen;
    }

    /**
     * Blocks until there is capacity for another write-cache entry.
     */
    private void awaitCapacity() {
        if (maxNumberOfKeysInSegmentWriteCache <= 0) {
            return;
        }
        capacityLock.lock();
        try {
            while (currentBufferedKeys() >= maxNumberOfKeysInSegmentWriteCache) {
                capacityAvailable.await();
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for write cache capacity", ex);
        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Signals waiting writers that capacity may be available.
     */
    private void signalCapacityAvailable() {
        capacityLock.lock();
        try {
            capacityAvailable.signalAll();
        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Builds a merged cache view (delta + frozen + write).
     *
     * @return merged cache instance
     */
    private UniqueCache<K, V> buildMergedCache() {
        final UniqueCache<K, V> merged = UniqueCache.<K, V>builder()
                .withKeyComparator(keyComparator).buildEmpty();
        addAll(merged, deltaCache.getAsList());
        if (frozenWriteCache != null && !frozenWriteCache.isEmpty()) {
            addAll(merged, frozenWriteCache.getAsList());
        }
        addAll(merged, writeCache.getAsList());
        return merged;
    }

    /**
     * Builds a new write cache with the configured capacity hint.
     *
     * @return empty write cache
     */
    private UniqueCache<K, V> buildWriteCache() {
        final int capacityHint = Math.min(
                Math.max(1, maxNumberOfKeysInSegmentWriteCacheDuringFlush),
                MAX_INITIAL_CAPACITY);
        return UniqueCache.<K, V>builder()//
                .withKeyComparator(keyComparator)//
                .withInitialCapacity(capacityHint)//
                .withThreadSafe(true)//
                .buildEmpty();
    }

    /**
     * Adds all non-null entries into the target cache.
     *
     * @param target cache to populate
     * @param entries entries to add (may be null)
     */
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
