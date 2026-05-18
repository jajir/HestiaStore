package org.hestiastore.index.segmentregistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * High-throughput, bounded cache with per-key loading and unloading control.
 * <p>
 * Concurrency design:
 * <ul>
 * <li>Map lookups are lock-free via {@link ConcurrentHashMap}.</li>
 * <li>Each entry has its own lock and condition, so unrelated keys never
 * block each other.</li>
 * <li>Only the winning thread loads a missing entry; other threads wait on
 * the entry condition.</li>
 * <li>Eviction picks the least recently used READY entry and marks it as
 * UNLOADING before closing the segment outside the locks.</li>
 * <li>Registry contract treats LOADING and UNLOADING differently:
 * LOADING is awaited on the same key, UNLOADING is surfaced as BUSY to
 * callers by registry layer decisions.</li>
 * </ul>
 *
 * @param <K> index key type
 * @param <V> index value type
 */
final class SegmentRegistryCache<K, V> {

    private final ConcurrentHashMap<SegmentId, Entry<Segment<K, V>>> map = new ConcurrentHashMap<>();
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicLong accessCx = new AtomicLong();
    private final ReentrantLock evictionLock = new ReentrantLock();
    private final AtomicInteger limit;
    private final SegmentLoadCloseOperations<K, V> segmentOperations;
    private final SegmentUnloadEligibility unloadEligibility;
    private final Executor unloadExecutor;
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder loadCount = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();

    /**
     * Creates a cache with direct segment load/close operations.
     *
     * @param limit             maximum number of cached entries
     * @param segmentOperations segment load/close operations
     * @param unloadEligibility unload eligibility policy
     * @param unloadExecutor    executor used for asynchronous eviction
     */
    SegmentRegistryCache(final int limit,
            final SegmentLoadCloseOperations<K, V> segmentOperations,
            final SegmentUnloadEligibility unloadEligibility,
            final Executor unloadExecutor) {
        this.limit = new AtomicInteger(
                Vldtn.requireGreaterThanZero(limit, "limit"));
        this.segmentOperations = Vldtn.requireNonNull(segmentOperations,
                "segmentOperations");
        this.unloadEligibility = Vldtn.requireNonNull(unloadEligibility,
                "unloadEligibility");
        this.unloadExecutor = Vldtn.requireNonNull(unloadExecutor,
                "unloadExecutor");
    }

    /**
     * Returns the cached value for the provided key, loading it if missing.
     *
     * @param key cache key
     * @return cached or newly loaded value
     */
    Segment<K, V> get(final SegmentId key) {
        Vldtn.requireNonNull(key, "key");
        while (true) {
            final long currentAccessCx = accessCx.getAndIncrement();
            Entry<Segment<K, V>> entry = map.get(key);
            if (entry == null) {
                missCount.increment();
                final Entry<Segment<K, V>> created = new Entry<>(
                        currentAccessCx);
                final Entry<Segment<K, V>> entryInMap = map.putIfAbsent(key,
                        created);
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
            final Segment<K, V> value = entry
                    .waitWhileLoading(currentAccessCx);
            if (value != null) {
                hitCount.increment();
                return value;
            }
            // Entry was unloaded; retry with a fresh lookup.
        }
    }

    Optional<Segment<K, V>> getIfReady(final SegmentId key) {
        Vldtn.requireNonNull(key, "key");
        final Entry<Segment<K, V>> entry = map.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(entry.getReadyValue());
    }

    InvalidateStatus invalidate(final SegmentId key) {
        Vldtn.requireNonNull(key, "key");
        final Entry<Segment<K, V>> entry = map.get(key);
        if (entry == null) {
            return InvalidateStatus.REMOVED;
        }
        final Segment<K, V> readyValue = entry.getReadyValue();
        if (readyValue == null || !unloadEligibility.canUnload(readyValue)
                || !entry.tryStartUnload(readyValue)) {
            return InvalidateStatus.BUSY;
        }
        final Segment<K, V> value = entry.getValueForUnload();
        if (value == null) {
            entry.cancelUnload();
            return InvalidateStatus.BUSY;
        }
        if (!unloadValue(value)) {
            entry.cancelUnload();
            return InvalidateStatus.BUSY;
        }
        return finalizeRemoval(key, entry, false) ? InvalidateStatus.REMOVED
                : InvalidateStatus.BUSY;
    }

    void clear() {
        for (final SegmentId key : map.keySet()) {
            invalidate(key);
        }
    }

    int getSize() {
        return size.get();
    }

    SegmentRegistryCacheStats metricsSnapshot() {
        return new SegmentRegistryCacheStats(hitCount.sum(), missCount.sum(),
                loadCount.sum(), evictionCount.sum(), size.get(), limit.get());
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    int getLimit() {
        return limit.get();
    }

    boolean updateLimit(final int newLimit) {
        limit.set(Vldtn.requireGreaterThanZero(newLimit, "newLimit"));
        while (size.get() > limit.get()) {
            if (!removeLastRecentUsedSegment(null)) {
                return false;
            }
        }
        return true;
    }

    List<Segment<K, V>> readyValuesSnapshot() {
        final List<Segment<K, V>> values = new ArrayList<>();
        for (final Entry<Segment<K, V>> entry : map.values()) {
            final Segment<K, V> readyValue = entry.getReadyValue();
            if (readyValue != null) {
                values.add(readyValue);
            }
        }
        return List.copyOf(values);
    }

    private Segment<K, V> loadValue(final SegmentId key,
            final Entry<Segment<K, V>> entry) {
        final Segment<K, V> value;
        try {
            value = Vldtn.requireNonNull(segmentOperations.loadSegment(key),
                    "loadedValue");
        } catch (final RuntimeException ex) {
            entry.fail(ex);
            map.remove(key, entry);
            throw ex;
        }
        entry.finishLoad(value);
        loadCount.increment();
        size.incrementAndGet();
        evictIfNeeded(key);
        return value;
    }

    private void evictIfNeeded(final SegmentId exceptKey) {
        if (size.get() <= limit.get()) {
            return;
        }
        removeLastRecentUsedSegment(exceptKey);
    }

    boolean removeLastRecentUsedSegment(final SegmentId exceptKey) {
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
            final SegmentId exceptKey) {
        long oldestAccessCx = Long.MAX_VALUE;
        SegmentId oldestKey = null;
        Entry<Segment<K, V>> oldestEntry = null;
        for (final Map.Entry<SegmentId, Entry<Segment<K, V>>> mapEntry : map
                .entrySet()) {
            final SegmentId key = mapEntry.getKey();
            if (exceptKey == null || !exceptKey.equals(key)) {
                final Entry<Segment<K, V>> entry = mapEntry.getValue();
                final long entryAccessCx = getEvictionOrder(entry);
                if (entryAccessCx != Long.MAX_VALUE
                        && entryAccessCx < oldestAccessCx) {
                    oldestAccessCx = entryAccessCx;
                    oldestKey = key;
                    oldestEntry = entry;
                }
            }
        }
        if (oldestEntry == null || oldestKey == null) {
            return null;
        }
        final Segment<K, V> value = oldestEntry.getReadyValue();
        if (value == null || !unloadEligibility.canUnload(value)
                || !oldestEntry.tryStartUnload(value)) {
            return null;
        }
        return new EvictionCandidate<>(oldestKey, oldestEntry, value);
    }

    private long getEvictionOrder(final Entry<Segment<K, V>> entry) {
        final Segment<K, V> value = entry.getReadyValue();
        if (value == null || !unloadEligibility.canUnload(value)) {
            return Long.MAX_VALUE;
        }
        return entry.getEvictionOrder(value);
    }

    private boolean unloadValue(final Segment<K, V> value) {
        try {
            segmentOperations.closeSegmentIfNeeded(value);
            return true;
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private void startUnloadAsync(final EvictionCandidate<K, V> candidate) {
        try {
            unloadExecutor.execute(() -> {
                if (!unloadValue(candidate.value)) {
                    candidate.entry.cancelUnload();
                    return;
                }
                finalizeRemoval(candidate.key, candidate.entry, true);
            });
        } catch (final RejectedExecutionException ex) {
            candidate.entry.cancelUnload();
        }
    }

    private boolean finalizeRemoval(final SegmentId key,
            final Entry<Segment<K, V>> entry, final boolean eviction) {
        final boolean removed = map.remove(key, entry);
        if (removed) {
            size.decrementAndGet();
            entry.finishUnload();
            if (eviction) {
                evictionCount.increment();
            }
        }
        return removed;
    }

    enum InvalidateStatus {
        REMOVED,
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

        long getEvictionOrder(final V expectedValue) {
            lock.lock();
            try {
                if (state != EntryState.READY || value == null) {
                    return Long.MAX_VALUE;
                }
                if (value != expectedValue) {
                    return Long.MAX_VALUE;
                }
                return accessCx;
            } finally {
                lock.unlock();
            }
        }

        boolean tryStartUnload(final V expectedValue) {
            lock.lock();
            try {
                if (state != EntryState.READY || value == null) {
                    return false;
                }
                if (value != expectedValue) {
                    return false;
                }
                state = EntryState.UNLOADING;
                return true;
            } finally {
                lock.unlock();
            }
        }

        V getValueForUnload() {
            lock.lock();
            try {
                if (state != EntryState.UNLOADING) {
                    return null;
                }
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

        void cancelUnload() {
            lock.lock();
            try {
                if (state != EntryState.UNLOADING || value == null) {
                    return;
                }
                state = EntryState.READY;
                ready.signalAll();
            } finally {
                lock.unlock();
            }
        }

        V getReadyValue() {
            lock.lock();
            try {
                if (state != EntryState.READY || value == null
                        || failure != null) {
                    return null;
                }
                return value;
            } finally {
                lock.unlock();
            }
        }
    }

    private static final class EvictionCandidate<K, V> {
        private final SegmentId key;
        private final Entry<Segment<K, V>> entry;
        private final Segment<K, V> value;

        private EvictionCandidate(final SegmentId key,
                final Entry<Segment<K, V>> entry,
                final Segment<K, V> value) {
            this.key = key;
            this.entry = entry;
            this.value = value;
        }
    }

}
