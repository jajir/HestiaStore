package org.hestiastore.index.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.hestiastore.index.Vldtn;

/**
 * Simple in-memory LRU cache that keeps track of the last access counter for
 * each entry and removes the least recently used element whenever the size
 * limit is reached. A {@link BiConsumer} can be supplied to observe evictions,
 * which is useful for releasing external resources when a cached value falls
 * out of the window.
 * This implementation is thread-safe; eviction is serialized while access
 * updates are lock-free, so LRU ordering is approximate under contention.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class CacheLru<K, V> implements Cache<K, V> {

    private final int limit;

    private final Map<K, CacheElement<V>> cache;

    private final BiConsumer<K, V> evictedElementConsumer;

    private final Object evictionLock = new Object();
    private final AtomicLong accessCx = new AtomicLong();

    public CacheLru(final int limit,
            final BiConsumer<K, V> evictedElementConsumer) {
        this.evictedElementConsumer = Vldtn.requireNonNull(
                evictedElementConsumer, "evictedElementConsumer");
        Vldtn.requireGreaterThanZero(limit, "limit");
        this.limit = limit;
        this.cache = new ConcurrentHashMap<>(limit);
    }

    @Override
    public void put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        cache.put(key,
                new CacheValueElement<V>(value, accessCx.getAndIncrement()));
        evictIfNeeded();
    }

    public void putNull(final K key) {
        Vldtn.requireNonNull(key, "key");
        cache.put(key, new CacheNullElement<V>(accessCx.getAndIncrement()));
        evictIfNeeded();
    }

    public CacheElement<V> getCacheElement(final K key) {
        final CacheElement<V> element = cache.get(key);
        if (element == null) {
            return null;
        }
        element.setCx(accessCx.getAndIncrement());
        return element;
    }

    @Override
    public Optional<V> get(final K key) {
        final CacheElement<V> element = cache.get(key);
        if (element == null) {
            return Optional.empty();
        }
        element.setCx(accessCx.getAndIncrement());
        return Optional.of(element.getValue());
    }

    private void evictIfNeeded() {
        if (cache.size() <= limit) {
            return;
        }
        K keyToRemove = null;
        CacheElement<V> removedElement = null;
        synchronized (evictionLock) {
            if (cache.size() <= limit) {
                return;
            }
            final Map.Entry<K, CacheElement<V>> oldest = findOldestEntry();
            if (oldest == null) {
                return;
            }
            final K candidateKey = oldest.getKey();
            final CacheElement<V> candidateElement = oldest.getValue();
            if (cache.remove(candidateKey, candidateElement)) {
                keyToRemove = candidateKey;
                removedElement = candidateElement;
            }
        }
        if (removedElement != null && !removedElement.isNull()) {
            evictedElementConsumer.accept(keyToRemove,
                    removedElement.getValue());
        }
    }

    private Map.Entry<K, CacheElement<V>> findOldestEntry() {
        long minCx = Long.MAX_VALUE;
        Map.Entry<K, CacheElement<V>> oldest = null;
        for (final Map.Entry<K, CacheElement<V>> entry : cache.entrySet()) {
            final CacheElement<V> element = entry.getValue();
            final long cx = element.getCx();
            if (cx < minCx) {
                minCx = cx;
                oldest = entry;
            }
        }
        return oldest;
    }

    @Override
    public void ivalidate(final K key) {
        Vldtn.requireNonNull(key, "key");
        final CacheElement<V> value = cache.remove(key);
        if (value != null && !value.isNull()) {
            evictedElementConsumer.accept(key, value.getValue());
        }
    }

    @Override
    public void invalidateAll() {
        cache.forEach((k, v) -> {
            if (v.isNull()) {
                return;
            }
            evictedElementConsumer.accept(k, v.getValue());
        });
        cache.clear();
    }

}
