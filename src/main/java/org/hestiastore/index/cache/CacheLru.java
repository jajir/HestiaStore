package org.hestiastore.index.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.hestiastore.index.Vldtn;

/**
 * Simple in-memory LRU cache that keeps track of the last access counter for
 * each entry and removes the least recently used element whenever the size
 * limit is reached. A {@link BiConsumer} can be supplied to observe evictions,
 * which is useful for releasing external resources when a cached value falls
 * out of the window.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class CacheLru<K, V> implements Cache<K, V> {

    private final int limit;

    private final Map<K, CacheElement<V>> cache;

    private final BiConsumer<K, V> evictedElementConsumer;

    private long accessCx = 0;

    public CacheLru(final int limit,
            final BiConsumer<K, V> evictedElementConsumer) {
        this.evictedElementConsumer = Vldtn.requireNonNull(
                evictedElementConsumer, "evictedElementConsumer");
        Vldtn.requireGreaterThanZero(limit, "limit");
        this.limit = limit;
        this.cache = new HashMap<>(limit);
    }

    @Override
    public void put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        optionalyRemoveSomeElements();
        cache.put(key, new CacheValueElement<V>(value, accessCx));
        accessCx++;
    }

    public void putNull(final K key) {
        Vldtn.requireNonNull(key, "key");
        optionalyRemoveSomeElements();
        cache.put(key, new CacheNullElement<V>(accessCx));
        accessCx++;
    }

    private void optionalyRemoveSomeElements() {
        if (cache.size() >= limit) {
            final K keyToRemove = getOlderElement();
            final CacheElement<V> element = cache.remove(keyToRemove);
            final V removedValue = element.getValue();
            evictedElementConsumer.accept(keyToRemove, removedValue);
        }
    }

    public CacheElement<V> getCacheElement(final K key) {
        final CacheElement<V> element = cache.get(key);
        if (element == null) {
            return null;
        }
        element.setCx(accessCx);
        accessCx++;
        return element;
    }

    @Override
    public Optional<V> get(final K key) {
        final CacheElement<V> element = cache.get(key);
        if (element == null) {
            return Optional.empty();
        }
        element.setCx(accessCx);
        accessCx++;
        return Optional.of(element.getValue());
    }

    private K getOlderElement() {
        long minCx = Long.MAX_VALUE;
        K minKey = null;
        for (final Map.Entry<K, CacheElement<V>> entry : cache.entrySet()) {
            final CacheElement<V> element = entry.getValue();
            if (element.getCx() < minCx) {
                minCx = element.getCx();
                minKey = entry.getKey();
            }
        }
        return minKey;
    }

    @Override
    public void ivalidate(final K key) {
        Vldtn.requireNonNull(key, "key");
        final CacheElement<V> value = cache.remove(key);
        if (value != null) {
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
