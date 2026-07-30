package org.hestiastore.index.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.hestiastore.index.Vldtn;

/**
 * Simple in-memory LRU cache backed by an access-ordered {@link LinkedHashMap}.
 * A {@link BiConsumer} can be supplied to observe evictions, which is useful
 * for releasing external resources when a cached value falls out of the
 * window.
 * This implementation is thread-safe; cache access is serialized so the LRU
 * order stays exact.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class CacheLruImpl<K, V> implements CacheLru<K, V> {

    private static final Object NULL_MARKER = new Object();

    private final int limit;

    private final LinkedHashMap<K, Object> cache;

    private final BiConsumer<K, V> evictedElementConsumer;

    /**
     * Creates an LRU cache with the supplied entry limit.
     *
     * @param limit                  maximum cached entry count
     * @param evictedElementConsumer consumer notified for evicted value entries
     */
    public CacheLruImpl(final int limit,
            final BiConsumer<K, V> evictedElementConsumer) {
        this.evictedElementConsumer = Vldtn.requireNonNull(
                evictedElementConsumer, "evictedElementConsumer");
        Vldtn.requireGreaterThanZero(limit, "limit");
        this.limit = limit;
        this.cache = new LinkedHashMap<>(limit, 0.75F, true);
    }

    @Override
    public void put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        final EvictedEntry<K, V> evictedEntry;
        synchronized (cache) {
            cache.put(key, value);
            evictedEntry = evictIfNeededLocked();
        }
        notifyEvicted(evictedEntry);
    }

    @Override
    public void putNull(final K key) {
        Vldtn.requireNonNull(key, "key");
        final EvictedEntry<K, V> evictedEntry;
        synchronized (cache) {
            cache.put(key, NULL_MARKER);
            evictedEntry = evictIfNeededLocked();
        }
        notifyEvicted(evictedEntry);
    }

    @Override
    public Optional<V> get(final K key) {
        Vldtn.requireNonNull(key, "key");
        final Object value;
        synchronized (cache) {
            value = cache.get(key);
        }
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(cachedValue(value));
    }

    private EvictedEntry<K, V> evictIfNeededLocked() {
        if (cache.size() <= limit) {
            return null;
        }
        final Iterator<Map.Entry<K, Object>> iterator =
                cache.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        final Map.Entry<K, Object> eldest = iterator.next();
        final Object eldestValue = eldest.getValue();
        iterator.remove();
        if (isNullMarker(eldestValue)) {
            return null;
        }
        return new EvictedEntry<>(eldest.getKey(), cachedValue(eldestValue));
    }

    @Override
    public void ivalidate(final K key) {
        Vldtn.requireNonNull(key, "key");
        final Object value;
        synchronized (cache) {
            value = cache.remove(key);
        }
        notifyEvicted(key, value);
    }

    @Override
    public void invalidateAll() {
        final List<EvictedEntry<K, V>> evictedEntries = new ArrayList<>();
        synchronized (cache) {
            cache.forEach((key, value) -> addEvictedEntry(evictedEntries, key,
                    value));
            cache.clear();
        }
        evictedEntries.forEach(
                entry -> evictedElementConsumer.accept(entry.key, entry.value));
    }

    private void addEvictedEntry(final List<EvictedEntry<K, V>> evictedEntries,
            final K key, final Object value) {
        if (!isNullMarker(value)) {
            evictedEntries.add(new EvictedEntry<>(key, cachedValue(value)));
        }
    }

    private void notifyEvicted(final K key, final Object value) {
        if (!isNullMarker(value)) {
            evictedElementConsumer.accept(key, cachedValue(value));
        }
    }

    private void notifyEvicted(final EvictedEntry<K, V> evictedEntry) {
        if (evictedEntry != null) {
            evictedElementConsumer.accept(evictedEntry.key, evictedEntry.value);
        }
    }

    private boolean isNullMarker(final Object value) {
        return value == null || value == NULL_MARKER;
    }

    @SuppressWarnings("unchecked")
    private V cachedValue(final Object value) {
        if (value == NULL_MARKER) {
            throw new IllegalStateException("Null element does not have a value");
        }
        return (V) value;
    }

    private static final class EvictedEntry<K, V> {
        private final K key;
        private final V value;

        private EvictedEntry(final K key, final V value) {
            this.key = key;
            this.value = value;
        }
    }

}
