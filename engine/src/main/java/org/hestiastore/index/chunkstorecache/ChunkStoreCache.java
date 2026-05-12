package org.hestiastore.index.chunkstorecache;

import java.util.Comparator;

/**
 * Index-scoped cache for parsed persisted chunk pages.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface ChunkStoreCache<K, V> {

    /**
     * Resolves a key through the parsed page cache.
     *
     * @param cacheKey page cache key
     * @param lookupKey key to find within the page
     * @param comparator key comparator
     * @param loader page loader used on misses
     * @return value when found, otherwise {@code null}
     */
    V find(ChunkStoreCacheKey cacheKey, K lookupKey, Comparator<K> comparator,
            ChunkPageLoader<K, V> loader);

    /**
     * Resolves a key through the parsed page cache.
     *
     * @param ownerId segment owner id
     * @param activeVersion active segment version
     * @param chunkPosition chunk start position
     * @param lookupKey key to find within the page
     * @param comparator key comparator
     * @param loader page loader used on misses
     * @return value when found, otherwise {@code null}
     */
    default V find(final String ownerId, final long activeVersion,
            final long chunkPosition, final K lookupKey,
            final Comparator<K> comparator,
            final ChunkPageLoader<K, V> loader) {
        return find(ChunkStoreCacheKey.of(ownerId, activeVersion, chunkPosition),
                lookupKey, comparator, loader);
    }

    /**
     * Updates the page limit.
     *
     * @param pageLimit maximum cached page count, or {@code 0} to disable
     */
    void updateLimit(int pageLimit);

    /**
     * Invalidates all pages owned by a segment.
     *
     * @param ownerId segment owner id
     */
    void invalidateOwner(String ownerId);

    /**
     * Clears all cached pages.
     */
    void clear();

    /**
     * Returns immutable cache statistics.
     *
     * @return cache stats snapshot
     */
    ChunkStoreCacheStats stats();

    /**
     * Returns whether cache storage is enabled.
     *
     * @return true when page limit is positive
     */
    boolean isEnabled();
}
