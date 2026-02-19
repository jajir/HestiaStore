package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
    private final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
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
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance write-cache size
     *        allowed during maintenance
     * @param maxNumberOfKeysInSegmentCache max delta-cache size hint
     */
    public SegmentCache(final Comparator<K> keyComparator,
            final TypeDescriptor<V> valueTypeDescriptor,
            final List<Entry<K, V>> deltaEntries,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
            final int maxNumberOfKeysInSegmentCache) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.maxNumberOfKeysInSegmentWriteCache = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = Vldtn
                .requireGreaterThanZero(
                        maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                        "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
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
            if (currentBufferedKeys() >= effectiveWriteCacheLimit()) {
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
        final Iterator<Entry<K, V>> iterator = mergedIterator();
        while (iterator.hasNext()) {
            final Entry<K, V> entry = iterator.next();
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
     * Returns a sorted iterator over the merged cache view.
     *
     * @return iterator over delta + frozen + write caches
     */
    Iterator<Entry<K, V>> mergedIterator() {
        return iteratorForCaches(deltaCache, frozenWriteCache, writeCache);
    }

    /**
     * Returns a sorted iterator over the stable compaction snapshot view.
     *
     * @return iterator over delta + frozen caches
     */
    Iterator<Entry<K, V>> compactionSnapshotIterator() {
        return iteratorForCaches(deltaCache, frozenWriteCache, null);
    }

    /**
     * Returns a sorted iterator over the frozen write cache.
     *
     * @return iterator over frozen write cache entries
     */
    Iterator<Entry<K, V>> frozenWriteCacheIterator() {
        if (frozenWriteCache == null || frozenWriteCache.isEmpty()) {
            return List.<Entry<K, V>>of().iterator();
        }
        final Iterator<K> keys = frozenWriteCache.getSortedKeyIterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                if (!keys.hasNext()) {
                    throw new NoSuchElementException("No next element.");
                }
                final K key = keys.next();
                final V value = frozenWriteCache.get(key);
                if (value == null) {
                    return next();
                }
                return Entry.of(key, value);
            }
        };
    }

    /**
     * Freezes the current write cache into a snapshot for flushing.
     *
     * @return sorted snapshot entries, possibly empty
     */
    void freezeWriteCache() {
        if (frozenWriteCache != null && !frozenWriteCache.isEmpty()) {
            return;
        }
        if (writeCache.isEmpty()) {
            return;
        }
        frozenWriteCache = writeCache;
        writeCache = buildWriteCache();
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
        frozenWriteCache.forEachEntry(
                (key, value) -> deltaCache.put(Entry.of(key, value)));
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
            while (currentBufferedKeys() >= effectiveWriteCacheLimit()) {
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
     * Returns the active write-cache limit for the current state.
     *
     * @return max buffered keys allowed right now
     */
    private int effectiveWriteCacheLimit() {
        if (frozenWriteCache != null) {
            return maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
        }
        return maxNumberOfKeysInSegmentWriteCache;
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

    private Iterator<Entry<K, V>> iteratorForCaches(
            final UniqueCache<K, V> delta,
            final UniqueCache<K, V> frozen,
            final UniqueCache<K, V> write) {
        final SourceCursor<K> deltaCursor = new SourceCursor<>(
                delta == null ? List.<K>of().iterator()
                        : delta.getSortedKeyIterator());
        final SourceCursor<K> frozenCursor = new SourceCursor<>(
                frozen == null ? List.<K>of().iterator()
                        : frozen.getSortedKeyIterator());
        final SourceCursor<K> writeCursor = new SourceCursor<>(
                write == null ? List.<K>of().iterator()
                        : write.getSortedKeyIterator());
        return new Iterator<>() {
            private Entry<K, V> next = advance();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Entry<K, V> next() {
                if (next == null) {
                    throw new NoSuchElementException("No next element.");
                }
                final Entry<K, V> out = next;
                next = advance();
                return out;
            }

            private Entry<K, V> advance() {
                while (deltaCursor.hasCurrent() || frozenCursor.hasCurrent()
                        || writeCursor.hasCurrent()) {
                    final K minKey = minKey(deltaCursor.current(),
                            frozenCursor.current(), writeCursor.current());
                    if (minKey == null) {
                        return null;
                    }
                    consumeIfEquals(deltaCursor, minKey);
                    consumeIfEquals(frozenCursor, minKey);
                    consumeIfEquals(writeCursor, minKey);

                    final V value = resolveValue(minKey, write, frozen, delta);
                    if (value == null) {
                        continue;
                    }
                    return Entry.of(minKey, value);
                }
                return null;
            }
        };
    }

    private void consumeIfEquals(final SourceCursor<K> cursor, final K key) {
        if (cursor.hasCurrent() && keyComparator.compare(cursor.current(), key) == 0) {
            cursor.advance();
        }
    }

    private K minKey(final K a, final K b, final K c) {
        K min = a;
        if (b != null && (min == null || keyComparator.compare(b, min) < 0)) {
            min = b;
        }
        if (c != null && (min == null || keyComparator.compare(c, min) < 0)) {
            min = c;
        }
        return min;
    }

    private V resolveValue(final K key, final UniqueCache<K, V> write,
            final UniqueCache<K, V> frozen, final UniqueCache<K, V> delta) {
        if (write != null) {
            final V value = write.get(key);
            if (value != null) {
                return value;
            }
        }
        if (frozen != null) {
            final V value = frozen.get(key);
            if (value != null) {
                return value;
            }
        }
        if (delta != null) {
            return delta.get(key);
        }
        return null;
    }

    private static final class SourceCursor<K> {
        private final Iterator<K> iterator;
        private K current;

        private SourceCursor(final Iterator<K> iterator) {
            this.iterator = iterator;
            this.current = iterator.hasNext() ? iterator.next() : null;
        }

        private boolean hasCurrent() {
            return current != null;
        }

        private K current() {
            return current;
        }

        private void advance() {
            current = iterator.hasNext() ? iterator.next() : null;
        }
    }

    /**
     * Builds a new write cache with the configured capacity hint.
     *
     * @return empty write cache
     */
    private UniqueCache<K, V> buildWriteCache() {
        final int capacityHint = Math.min(
                Math.max(1, maxNumberOfKeysInSegmentWriteCacheDuringMaintenance),
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
