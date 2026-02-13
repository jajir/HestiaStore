package org.hestiastore.index.segmentregistry;

import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
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
 *   <li>Registry contract treats LOADING and UNLOADING differently:
 *   LOADING is awaited on the same key, UNLOADING is surfaced as BUSY to
 *   callers by registry layer decisions.</li>
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
    private final ExecutorService unloadExecutor;

    /**
     * Creates a cache with a fixed size limit.
     *
     * @param limit   maximum number of cached entries
     * @param loader  value loader invoked on cache misses
     * @param unloader value unloader invoked on eviction/removal
     */
    public SegmentRegistryCache(final int limit, final Function<K, V> loader,
            final Consumer<V> unloader) {
        this(limit, loader, unloader, DirectExecutorService.INSTANCE);
    }

    SegmentRegistryCache(final int limit, final Function<K, V> loader,
            final Consumer<V> unloader,
            final ExecutorService unloadExecutor) {
        this.limit = Vldtn.requireGreaterThanZero(limit, "limit");
        this.loader = Vldtn.requireNonNull(loader, "loader");
        this.unloader = Vldtn.requireNonNull(unloader, "unloader");
        this.unloadExecutor = Vldtn.requireNonNull(unloadExecutor,
                "unloadExecutor");
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
                final Entry<V> entryInMap = map.putIfAbsent(key, created);
                if (entryInMap == null) {
                    // Winner path: created is now the value associated with key.
                    if (!created.tryStartLoad()) {
                        map.remove(key, created);
                        throw new IllegalStateException(
                                "Entry cannot start load from current state");
                    }
                    return loadValue(key, created);
                }
                // Loser path: always wait on the entry returned from the map.
                entry = entryInMap;
            }
            final V value = entry.waitWhileLoading(currentAccessCx);
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
        if (!unloadValueAndWait(value)) {
            return InvalidateStatus.BUSY;
        }
        return finalizeRemoval(key, entry) ? InvalidateStatus.REMOVED
                : InvalidateStatus.BUSY;
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
        evictIfNeeded(key);
        return value;
    }

    private void evictIfNeeded(final K exceptKey) {
        if (size.get() <= limit) {
            return;
        }
        removeLastRecentUsedSegment(exceptKey);
    }

    boolean removeLastRecentUsedSegment(final K exceptKey) {
        final EvictionCandidate<K, V> candidate;
        evictionLock.lock();
        try {
            candidate = selectLeastRecentlyUsedCandidate(exceptKey);
            if (candidate == null) {
                return false;
            }
        } finally {
            evictionLock.unlock();
        }
        startUnloadAsync(candidate);
        return true;
    }

    private EvictionCandidate<K, V> selectLeastRecentlyUsedCandidate(
            final K exceptKey) {
        final Set<K> excluded = new HashSet<>();
        if (exceptKey != null) {
            excluded.add(exceptKey);
        }
        while (true) {
            long oldestAccessCx = Long.MAX_VALUE;
            K oldestKey = null;
            Entry<V> oldestEntry = null;

            for (final Map.Entry<K, Entry<V>> mapEntry : map.entrySet()) {
                final K key = mapEntry.getKey();
                if (excluded.contains(key)) {
                    continue;
                }
                final Entry<V> entry = mapEntry.getValue();
                final long entryAccessCx = entry.getEvictionOrder();
                if (entryAccessCx == Long.MAX_VALUE) {
                    continue;
                }
                if (entryAccessCx < oldestAccessCx) {
                    oldestAccessCx = entryAccessCx;
                    oldestKey = key;
                    oldestEntry = entry;
                }
            }

            if (oldestEntry == null || oldestKey == null) {
                return null;
            }

            final V value = oldestEntry.tryStartUnload();
            if (value != null) {
                return new EvictionCandidate<>(oldestKey, oldestEntry, value);
            }
            // Candidate changed state between selection and unload start;
            // retry selection with a different candidate.
            excluded.add(oldestKey);
        }
    }

    private boolean unloadValue(final V value) {
        try {
            unloader.accept(value);
            return true;
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private boolean unloadValueAndWait(final V value) {
        final Future<?> task;
        try {
            task = unloadExecutor.submit(() -> {
                if (!unloadValue(value)) {
                    throw new IllegalStateException("Unload failed");
                }
            });
        } catch (final RejectedExecutionException ex) {
            return false;
        }
        try {
            task.get();
            return true;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        } catch (final ExecutionException ex) {
            return false;
        }
    }

    private void startUnloadAsync(final EvictionCandidate<K, V> candidate) {
        try {
            unloadExecutor.execute(() -> {
                if (!unloadValue(candidate.value)) {
                    // Keep entry in UNLOADING on close failure.
                    return;
                }
                finalizeRemoval(candidate.key, candidate.entry);
            });
        } catch (final RejectedExecutionException ex) {
            // Keep entry in UNLOADING if lifecycle executor is not accepting tasks.
        }
    }

    private boolean finalizeRemoval(final K key, final Entry<V> entry) {
        final boolean removed = map.remove(key, entry);
        if (removed) {
            size.decrementAndGet();
            entry.finishUnload();
        }
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

    static final class EntryBusyException extends RuntimeException {
        private static final long serialVersionUID = 1L;
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

        boolean tryStartLoad() {
            lock.lock();
            try {
                return state == EntryState.LOADING && value == null
                        && failure == null;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Waits for this key while loading is in progress and returns the
         * currently visible value.
         *
         * @param currentAccessCx access sequence for recency tracking
         * @return loaded value when READY, otherwise null when entry became
         *         unavailable
         */
        V waitWhileLoading(final long currentAccessCx) {
            lock.lock();
            try {
                while (state == EntryState.LOADING) {
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
                if (state == EntryState.UNLOADING) {
                    throw new EntryBusyException();
                }
                return null;
            } finally {
                lock.unlock();
            }
        }

        void finishLoad(final V value) {
            lock.lock();
            try {
                if (state != EntryState.LOADING || this.value != null
                        || failure != null) {
                    throw new IllegalStateException(
                            "Invalid transition to READY from " + state);
                }
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
                if (state != EntryState.LOADING || this.value != null
                        || this.failure != null) {
                    throw new IllegalStateException(
                            "Invalid fail transition from " + state);
                }
                this.failure = failure;
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
                if (state != EntryState.UNLOADING) {
                    throw new IllegalStateException(
                            "Invalid transition to missing from " + state);
                }
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

    private static final class DirectExecutorService
            extends AbstractExecutorService {
        private static final DirectExecutorService INSTANCE = new DirectExecutorService();
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(final long timeout,
                final TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(final Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException(
                        "Direct executor is shutdown");
            }
            command.run();
        }
    }
}
