package org.hestiastore.index.segmentindex;

import java.util.Comparator;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.cache.UniqueCacheBuilder;

final class WriteCache<K, V> {

    private final Comparator<K> comparator;
    private final Integer capacity;
    private volatile UniqueCache<K, V> activeCache;
    private volatile UniqueCache<K, V> flushingCache;

    WriteCache(final Comparator<K> comparator, final Integer capacity) {
        this.comparator = Vldtn.requireNonNull(comparator, "comparator");
        this.capacity = capacity;
        this.activeCache = newCache();
        this.flushingCache = null;
    }

    UniqueCache<K, V> getActiveCache() {
        return activeCache;
    }

    UniqueCache<K, V> getFlushingCache() {
        return flushingCache;
    }

    int activeSize() {
        return activeCache.size();
    }

    void put(final Entry<K, V> entry) {
        final UniqueCache<K, V> cacheRef = activeCache;
        cacheRef.put(entry);
        if (cacheRef != activeCache) {
            activeCache.put(entry);
        }
    }

    V get(final K key) {
        final V value = activeCache.get(key);
        if (value != null) {
            return value;
        }
        final UniqueCache<K, V> flushing = flushingCache;
        if (flushing == null) {
            return null;
        }
        return flushing.get(key);
    }

    UniqueCache<K, V> swapForFlush() {
        final UniqueCache<K, V> toFlush = activeCache;
        flushingCache = toFlush;
        activeCache = newCache();
        return toFlush;
    }

    void clearFlushing() {
        flushingCache = null;
    }

    void replaceActiveCacheForTest(final UniqueCache<K, V> cache) {
        this.activeCache = Vldtn.requireNonNull(cache, "cache");
        this.flushingCache = null;
    }

    private UniqueCache<K, V> newCache() {
        final UniqueCacheBuilder<K, V> cacheBuilder = UniqueCache
                .<K, V>builder()
                .withKeyComparator(comparator)
                .withThreadSafe(true);
        if (capacity != null && capacity > 0) {
            cacheBuilder.withInitialCapacity(capacity);
        }
        return cacheBuilder.buildEmpty();
    }
}
