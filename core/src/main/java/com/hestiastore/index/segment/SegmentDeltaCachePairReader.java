package com.hestiastore.index.segment;

import java.util.Iterator;
import java.util.Objects;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.Pair;
import com.hestiastore.index.cache.UniqueCache;

/**
 * Read pair from cache. Values are always latest, but kyts
 */
public class SegmentDeltaCachePairReader<K, V>
        implements CloseablePairReader<K, V> {

    private final UniqueCache<K, V> cache;
    private final Iterator<K> cacheKeyIterator;

    SegmentDeltaCachePairReader(final UniqueCache<K, V> cache) {
        this.cache = Objects.requireNonNull(cache);
        this.cacheKeyIterator = cache.getSortedKeys().iterator();
    }

    @Override
    public Pair<K, V> read() {
        if (cacheKeyIterator.hasNext()) {
            final K key = cacheKeyIterator.next();
            return Pair.of(key, cache.get(key));
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        cacheKeyIterator.forEachRemaining(i -> {
            // intentionally do nothing
        });
    }

}
