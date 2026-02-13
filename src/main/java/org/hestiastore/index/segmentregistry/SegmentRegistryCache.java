package org.hestiastore.index.segmentregistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hestiastore.index.Vldtn;

/**
 * High-throughput, bounded cache with per-key loading and unloading control.
 * <p>
 * Concurrency design:
 * <ul>
 *   <li>Map lookups are lock-free via {@link ConcurrentHashMap}.</li>
 *   <li>Each entry has its own lock and condition, so unrelated keys never
 *   block each other.</li>
 *   <li>Only the winning thread loads a missing entry; other threads wait on
 *   the entry condition.</li>
 *   <li>Eviction picks the least recently used READY entry and marks it as
 *   UNLOADING before calling the unloader outside the locks.</li>
 * </ul>
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentRegistryCache<K, V> {

    private final ConcurrentHashMap<K, Entry<V>> map = new ConcurrentHashMap<>();
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicLong accessCx = new AtomicLong();
    private final ReentrantLock evictionLock = new ReentrantLock();
    private final int limit;
    private final Function<K, V> loader;
    private final Consumer<V> unloader;

    /**
     * Creates a cache with a fixed size limit.
     *
     * @param limit   maximum number of cached entries
     * @param loader  value loader invoked on cache misses
     * @param unloader value unloader invoked on eviction/removal
     */
    public SegmentRegistryCache(final int limit, final Function<K, V> loader,
            final Consumer<V> unloader) {
        this.limit = Vldtn.requireGreaterThanZero(limit, "limit");
        this.loader = Vldtn.requireNonNull(loader, "loader");
        this.unloader = Vldtn.requireNonNull(unloader, "unloader");
    }

    /**
     * Returns the cached value for the provided key, loading it if missing.
     *
     * @param key cache key
     * @return cached or newly loaded value
     */
    public V get(final K key) {
        Vldtn.requireNonNull(key, "key");
        while (true) {
            final long currentAccessCx = accessCx.getAndIncrement();
            Entry<V> entry = map.get(key);
            if (entry == null) {
                final Entry<V> created = new Entry<>(currentAccessCx);
                final Entry<V> existing = map.putIfAbsent(key, created);
                if (existing == null) {
                    return loadValue(key, created);
                }
                entry = existing;
            }
            final V value = entry.awaitReady(currentAccessCx);
            if (value != null) {
                return value;
            }
            // Entry was unloaded; retry with a fresh lookup.
        }
    }

    void retain(final K key) {
        final Entry<V> entry = map.get(key);
        if (entry != null) {
            entry.retain();
        }
    }

    void release(final K key) {
        final Entry<V> entry = map.get(key);
        if (entry != null) {
            entry.release();
        }
    }

    InvalidateStatus invalidate(final K key) {
        Vldtn.requireNonNull(key, "key");
        final Entry<V> entry = map.get(key);
        if (entry == null) {
            return InvalidateStatus.NOT_FOUND;
        }
        final V value = entry.tryStartUnload();
        if (value == null) {
            return InvalidateStatus.BUSY;
        }
        unloadValue(value);
        finalizeRemoval(key, entry);
        return InvalidateStatus.REMOVED;
    }

    void clear() {
        for (final K key : map.keySet()) {
            invalidate(key);
        }
    }

    int getSize() {
        return size.get();
    }

    private V loadValue(final K key, final Entry<V> entry) {
        final V value;
        try {
            value = Vldtn.requireNonNull(loader.apply(key), "loadedValue");
        } catch (final RuntimeException ex) {
            entry.fail(ex);
            map.remove(key, entry);
            throw ex;
        }
        entry.finishLoad(value);
        size.incrementAndGet();
        evictIfNeeded();
        return value;
    }

    private void evictIfNeeded() {
        while (size.get() > limit) {
            if (!evictLeastRecentlyUsed()) {
                return;
            }
        }
    }

    private boolean evictLeastRecentlyUsed() {
        final EvictionCandidate<K, V> candidate;
        evictionLock.lock();
        try {
            if (size.get() <= limit) {
                return true;
            }
            candidate = selectLeastRecentlyUsedCandidate();
            if (candidate == null) {
                return false;
            }
        } finally {
            evictionLock.unlock();
        }
        unloadValue(candidate.value);
        return finalizeRemoval(candidate.key, candidate.entry);
    }

    private EvictionCandidate<K, V> selectLeastRecentlyUsedCandidate() {
        long oldestAccessCx = Long.MAX_VALUE;
        K oldestKey = null;
        Entry<V> oldestEntry = null;

        for (final Map.Entry<K, Entry<V>> mapEntry : map.entrySet()) {
            final Entry<V> entry = mapEntry.getValue();
            final long entryAccessCx = entry.getEvictionOrder();
            if (entryAccessCx == Long.MAX_VALUE) {
                continue;
            }
            if (entryAccessCx < oldestAccessCx) {
                oldestAccessCx = entryAccessCx;
                oldestKey = mapEntry.getKey();
                oldestEntry = entry;
            }
        }

        if (oldestEntry == null) {
            return null;
        }

        final V value = oldestEntry.tryStartUnload();
        if (value == null) {
            return null;
        }
        return new EvictionCandidate<>(oldestKey, oldestEntry, value);
    }

    private void unloadValue(final V value) {
        try {
            unloader.accept(value);
        } catch (final RuntimeException ex) {
            // Best-effort unload; eviction continues even if unload fails.
        }
    }

    private boolean finalizeRemoval(final K key, final Entry<V> entry) {
        final boolean removed = map.remove(key, entry);
        if (removed) {
            size.decrementAndGet();
        }
        entry.finishUnload();
        return removed;
    }

    enum InvalidateStatus {
        REMOVED,
        NOT_FOUND,
        BUSY
    }

    enum EntryState {
        LOADING,
        READY,
        UNLOADING
    }

    static final class Entry<V> {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition ready = lock.newCondition();
        private volatile long accessCx;
        private EntryState state = EntryState.LOADING;
        private V value;
        private int refCount;
        private RuntimeException failure;

        Entry(final long accessCx) {
            this.accessCx = accessCx;
        }

        V awaitReady(final long currentAccessCx) {
            lock.lock();
            try {
                while (state == EntryState.LOADING
                        || (state == EntryState.UNLOADING && value != null)) {
                    if (failure != null) {
                        throw failure;
                    }
                    try {
                        ready.await();
                    } catch (final InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Interrupted while waiting for cache entry",
                                ex);
                    }
                }
                if (failure != null) {
                    throw failure;
                }
                if (state == EntryState.READY) {
                    accessCx = currentAccessCx;
                    return value;
                }
                return null;
            } finally {
                lock.unlock();
            }
        }

        void finishLoad(final V value) {
            lock.lock();
            try {
                this.value = value;
                this.state = EntryState.READY;
                ready.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void fail(final RuntimeException failure) {
            lock.lock();
            try {
                this.failure = failure;
                this.state = EntryState.UNLOADING;
                ready.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void retain() {
            lock.lock();
            try {
                if (state == EntryState.READY && value != null) {
                    refCount++;
                }
            } finally {
                lock.unlock();
            }
        }

        void release() {
            lock.lock();
            try {
                if (refCount > 0) {
                    refCount--;
                    if (refCount == 0) {
                        ready.signalAll();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        long getEvictionOrder() {
            lock.lock();
            try {
                if (state != EntryState.READY || value == null || refCount > 0) {
                    return Long.MAX_VALUE;
                }
                return accessCx;
            } finally {
                lock.unlock();
            }
        }

        V tryStartUnload() {
            lock.lock();
            try {
                if (state != EntryState.READY || value == null) {
                    return null;
                }
                if (refCount > 0) {
                    return null;
                }
                state = EntryState.UNLOADING;
                return value;
            } finally {
                lock.unlock();
            }
        }

        void finishUnload() {
            lock.lock();
            try {
                value = null;
                ready.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private static final class EvictionCandidate<K, V> {
        private final K key;
        private final Entry<V> entry;
        private final V value;

        private EvictionCandidate(final K key, final Entry<V> entry,
                final V value) {
            this.key = key;
            this.entry = entry;
            this.value = value;
        }
    }
}
