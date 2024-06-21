package com.coroptis.index.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Simple implementation of LRU algorithm.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class CacheLru<K, V> implements Cache<K, V> {

    private final int limit;

    private final Map<K, CacheLruElement<V>> cache;

    private final BiConsumer<K, V> evictedElement;

    private long accessCx = 0;

    public CacheLru(final int limit, final BiConsumer<K, V> evictedElement) {
        this.limit = limit;
        this.evictedElement = Objects.requireNonNull(evictedElement);
        this.cache = new HashMap<>(limit);
    }

    @Override
    public void put(final K key, final V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        if (cache.size() >= limit) {
            final K keyToRemove = getOlderElement();
            final CacheLruElement<V> element = cache.remove(keyToRemove);
            final V removedValue = element.getValue();
            evictedElement.accept(keyToRemove, removedValue);
        }
        cache.put(key, new CacheLruElement<V>(value, accessCx));
        accessCx++;
    }

    @Override
    public Optional<V> get(final K key) {
        final CacheLruElement<V> element = cache.get(key);
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
        for (final Map.Entry<K, CacheLruElement<V>> entry : cache.entrySet()) {
            final CacheLruElement<V> element = entry.getValue();
            if (element.getCx() < minCx) {
                minCx = element.getCx();
                minKey = entry.getKey();
            }
        }
        return minKey;
    }

    @Override
    public void ivalidate(final K key) {
        Objects.requireNonNull(key);
        cache.remove(key);
    }

}
