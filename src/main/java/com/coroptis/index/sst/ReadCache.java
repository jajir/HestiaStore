package com.coroptis.index.sst;

import java.util.Optional;

import com.coroptis.index.cache.Cache;
import com.coroptis.index.cache.CacheLru;

public class ReadCache<K, V> {

    private Cache<K, V> cache;

    public ReadCache() {
        evict();
    }

    void put(final K key, final V value) {
        cache.put(key, value);
    }

    Optional<V> get(final K key) {
        return cache.get(key);
    }

    void evict() {
        // FIXME number items should be configurable
        cache = new CacheLru<>(10_000_000, (key, value) -> {
            // intentionally do nothing
        });
    }
}
