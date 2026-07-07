package org.hestiastore.index.chunkstorecache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Page-count bounded LRU implementation of {@link ChunkStoreCache}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class LruChunkStoreCache<K, V> implements ChunkStoreCache<K, V> {

    private final Object monitor = new Object();
    private final LinkedHashMap<ChunkStoreCacheKey, ParsedChunkPage<K, V>> pages =
            new LinkedHashMap<>(16, 0.75F, true);
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();
    private final AtomicLong loadCount = new AtomicLong();
    private final AtomicLong evictionCount = new AtomicLong();
    private final AtomicLong invalidationCount = new AtomicLong();
    private volatile int pageLimit;
    private long entryCount;

    /**
     * Creates a cache with the supplied page limit.
     *
     * @param pageLimit maximum cached page count, or {@code 0} to disable
     */
    public LruChunkStoreCache(final int pageLimit) {
        this.pageLimit = Vldtn.requireGreaterThanOrEqualToZero(pageLimit,
                "pageLimit");
    }

    @Override
    public V find(final ChunkStoreCacheKey cacheKey, final K lookupKey,
            final Comparator<K> comparator,
            final Supplier<ParsedChunkPage<K, V>> loader) {
        final ChunkStoreCacheKey resolvedKey = Vldtn.requireNonNull(cacheKey,
                "cacheKey");
        final K resolvedLookupKey = Vldtn.requireNonNull(lookupKey,
                "lookupKey");
        final Comparator<K> resolvedComparator = Vldtn
                .requireNonNull(comparator, "comparator");
        final Supplier<ParsedChunkPage<K, V>> resolvedLoader = Vldtn
                .requireNonNull(loader, "loader");
        if (!isEnabled()) {
            return resolvedLoader.get().find(resolvedLookupKey,
                    resolvedComparator);
        }
        final ParsedChunkPage<K, V> cachedPage = getCachedPage(resolvedKey);
        if (cachedPage != null) {
            hitCount.incrementAndGet();
            return cachedPage.find(resolvedLookupKey, resolvedComparator);
        }
        missCount.incrementAndGet();
        final ParsedChunkPage<K, V> loadedPage = Vldtn.requireNonNull(
                resolvedLoader.get(), "loadedPage");
        loadCount.incrementAndGet();
        putCachedPage(resolvedKey, loadedPage);
        return loadedPage.find(resolvedLookupKey, resolvedComparator);
    }

    @Override
    public void updateLimit(final int limit) {
        final int resolvedLimit = Vldtn.requireGreaterThanOrEqualToZero(limit,
                "pageLimit");
        synchronized (monitor) {
            pageLimit = resolvedLimit;
            if (resolvedLimit == 0) {
                clearLocked();
                return;
            }
            evictToLimit();
        }
    }

    @Override
    public void invalidateOwner(final String ownerId) {
        final String resolvedOwnerId = Vldtn.requireNotBlank(ownerId,
                "ownerId");
        synchronized (monitor) {
            int removed = 0;
            final Iterator<Map.Entry<ChunkStoreCacheKey, ParsedChunkPage<K, V>>> iterator =
                    pages.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<ChunkStoreCacheKey, ParsedChunkPage<K, V>> entry =
                        iterator.next();
                if (resolvedOwnerId.equals(entry.getKey().ownerId())) {
                    entryCount -= entry.getValue().size();
                    iterator.remove();
                    removed++;
                }
            }
            if (removed > 0) {
                invalidationCount.addAndGet(removed);
            }
        }
    }

    @Override
    public void clear() {
        synchronized (monitor) {
            clearLocked();
        }
    }

    @Override
    public ChunkStoreCacheStats stats() {
        synchronized (monitor) {
            return new ChunkStoreCacheStats(pageLimit, pages.size(), entryCount,
                    hitCount.get(), missCount.get(), loadCount.get(),
                    evictionCount.get(), invalidationCount.get());
        }
    }

    @Override
    public boolean isEnabled() {
        return pageLimit > 0;
    }

    private ParsedChunkPage<K, V> getCachedPage(
            final ChunkStoreCacheKey cacheKey) {
        synchronized (monitor) {
            return pages.get(cacheKey);
        }
    }

    private void putCachedPage(final ChunkStoreCacheKey cacheKey,
            final ParsedChunkPage<K, V> page) {
        synchronized (monitor) {
            if (!isEnabled()) {
                return;
            }
            final ParsedChunkPage<K, V> previous = pages.put(cacheKey, page);
            if (previous != null) {
                entryCount -= previous.size();
            }
            entryCount += page.size();
            evictToLimit();
        }
    }

    private void evictToLimit() {
        while (pages.size() > pageLimit) {
            final Iterator<Map.Entry<ChunkStoreCacheKey, ParsedChunkPage<K, V>>> iterator =
                    pages.entrySet().iterator();
            if (!iterator.hasNext()) {
                return;
            }
            final Map.Entry<ChunkStoreCacheKey, ParsedChunkPage<K, V>> eldest =
                    iterator.next();
            entryCount -= eldest.getValue().size();
            iterator.remove();
            evictionCount.incrementAndGet();
        }
    }

    private void clearLocked() {
        final int removed = pages.size();
        pages.clear();
        entryCount = 0L;
        if (removed > 0) {
            invalidationCount.addAndGet(removed);
        }
    }
}
