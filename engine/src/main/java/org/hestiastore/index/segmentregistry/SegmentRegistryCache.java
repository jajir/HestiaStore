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
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
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
 * <li>Loads reserve a cache slot before building the segment; when the cache
 * is full, dirty victims are closed only under capacity pressure instead of
 * allocating unbounded extra segments.</li>
 * <li>Registry contract treats LOADING and UNLOADING differently:
 * LOADING is awaited on the same key, UNLOADING is surfaced as BUSY to
 * callers by registry layer decisions.</li>
 * </ul>
 *
 * @param <K> index key type
 * @param <V> index value type
 */
final class SegmentRegistryCache<K, V> {

    private final ConcurrentHashMap<SegmentId, SegmentRegistryEntry<K, V>> map =
            new ConcurrentHashMap<>();
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
            SegmentRegistryEntry<K, V> entry = map.get(key);
            if (entry == null) {
                missCount.increment();
                final SegmentRegistryEntry<K, V> created = new SegmentRegistryEntry<>(
                        currentAccessCx);
                final SegmentRegistryEntry<K, V> entryInMap = map
                        .putIfAbsent(key, created);
                if (entryInMap == null) {
                    // Winner path: created is now the value associated with key.
                    if (!created.tryStartLoad()) {
                        map.remove(key, created);
                        throw new IndexException(
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
        final SegmentRegistryEntry<K, V> entry = map.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(entry.getReadyValue());
    }

    InvalidateStatus invalidate(final SegmentId key) {
        Vldtn.requireNonNull(key, "key");
        final SegmentRegistryEntry<K, V> entry = map.get(key);
        if (entry == null) {
            return InvalidateStatus.REMOVED;
        }
        final Segment<K, V> readyValue = entry.getReadyValue();
        if (readyValue == null || !unloadEligibility.canUnload(readyValue)
                || !entry.tryStartUnload(readyValue)) {
            return InvalidateStatus.BUSY;
        }
        return unloadAndRemove(key, entry);
    }

    InvalidateStatus forceInvalidate(final SegmentId key) {
        Vldtn.requireNonNull(key, "key");
        final SegmentRegistryEntry<K, V> entry = map.get(key);
        if (entry == null) {
            return InvalidateStatus.REMOVED;
        }
        final Segment<K, V> readyValue = entry.getReadyValue();
        if (readyValue == null || !entry.tryStartUnload(readyValue)) {
            return InvalidateStatus.BUSY;
        }
        return unloadAndRemove(key, entry);
    }

    private InvalidateStatus unloadAndRemove(final SegmentId key,
            final SegmentRegistryEntry<K, V> entry) {
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
        return evictSynchronouslyAboveLimit(null);
    }

    List<Segment<K, V>> readyValuesSnapshot() {
        final List<Segment<K, V>> values = new ArrayList<>();
        for (final SegmentRegistryEntry<K, V> entry : map.values()) {
            final Segment<K, V> readyValue = entry.getReadyValue();
            if (readyValue != null) {
                values.add(readyValue);
            }
        }
        return List.copyOf(values);
    }

    private Segment<K, V> loadValue(final SegmentId key,
            final SegmentRegistryEntry<K, V> entry) {
        if (!reserveSlotForLoad(key)) {
            final SegmentBusyException failure = new SegmentBusyException();
            entry.fail(failure);
            map.remove(key, entry);
            throw failure;
        }
        boolean loaded = false;
        try {
            final Segment<K, V> value = Vldtn.requireNonNull(
                    segmentOperations.loadSegment(key), "loadedValue");
            entry.finishLoad(value);
            loaded = true;
            loadCount.increment();
            return value;
        } catch (final IndexException ex) {
            entry.fail(ex);
            throw ex;
        } catch (final RuntimeException ex) {
            final IndexException failure = new IndexException(
                    "Unable to load segment " + key, ex);
            entry.fail(failure);
            throw failure;
        } finally {
            if (!loaded) {
                cleanupFailedLoad(key, entry);
            }
        }
    }

    private void cleanupFailedLoad(final SegmentId key,
            final SegmentRegistryEntry<K, V> entry) {
        if (map.remove(key, entry)) {
            size.decrementAndGet();
        }
        entry.cancelLoadIfNeeded();
    }

    private boolean reserveSlotForLoad(final SegmentId exceptKey) {
        evictionLock.lock();
        try {
            while (size.get() >= limit.get()) {
                final EvictionCandidate<K, V> candidate =
                        selectCandidateForCapacityPressure(exceptKey);
                if (candidate == null || !unloadAndFinalize(candidate, true)) {
                    return false;
                }
            }
            size.incrementAndGet();
            return true;
        } finally {
            evictionLock.unlock();
        }
    }

    private EvictionCandidate<K, V> selectCandidateForCapacityPressure(
            final SegmentId exceptKey) {
        final EvictionCandidate<K, V> cleanCandidate =
                selectLeastRecentlyUsedCandidate(exceptKey, false);
        if (cleanCandidate != null) {
            return cleanCandidate;
        }
        return selectLeastRecentlyUsedCandidate(exceptKey, true);
    }

    boolean removeLastRecentUsedSegment(final SegmentId exceptKey) {
        final EvictionCandidate<K, V> candidate = selectEvictionCandidate(
                exceptKey);
        if (candidate == null) {
            return false;
        }
        return startUnloadAsync(candidate);
    }

    private boolean evictSynchronouslyAboveLimit(final SegmentId exceptKey) {
        while (size.get() > limit.get()) {
            final EvictionCandidate<K, V> candidate = selectEvictionCandidate(
                    exceptKey);
            if (candidate == null || !unloadAndFinalize(candidate, true)) {
                return false;
            }
        }
        return true;
    }

    private EvictionCandidate<K, V> selectEvictionCandidate(
            final SegmentId exceptKey) {
        evictionLock.lock();
        try {
            return selectLeastRecentlyUsedCandidate(exceptKey, false);
        } finally {
            evictionLock.unlock();
        }
    }

    private EvictionCandidate<K, V> selectLeastRecentlyUsedCandidate(
            final SegmentId exceptKey, final boolean force) {
        long oldestAccessCx = Long.MAX_VALUE;
        SegmentId oldestKey = null;
        SegmentRegistryEntry<K, V> oldestEntry = null;
        for (final Map.Entry<SegmentId, SegmentRegistryEntry<K, V>> mapEntry : map
                .entrySet()) {
            final SegmentId key = mapEntry.getKey();
            if (exceptKey == null || !exceptKey.equals(key)) {
                final SegmentRegistryEntry<K, V> entry = mapEntry
                        .getValue();
                final long entryAccessCx = getEvictionOrder(entry, force);
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
        if (value == null || !canSelectForUnload(value, force)
                || !oldestEntry.tryStartUnload(value)) {
            return null;
        }
        return new EvictionCandidate<>(oldestKey, oldestEntry, value);
    }

    private long getEvictionOrder(
            final SegmentRegistryEntry<K, V> entry,
            final boolean force) {
        final Segment<K, V> value = entry.getReadyValue();
        if (value == null || !canSelectForUnload(value, force)) {
            return Long.MAX_VALUE;
        }
        return entry.getEvictionOrder(value);
    }

    private boolean canSelectForUnload(final Segment<K, V> value,
            final boolean force) {
        return force ? unloadEligibility.canForceUnload(value)
                : unloadEligibility.canUnload(value);
    }

    private boolean unloadValue(final Segment<K, V> value) {
        try {
            segmentOperations.closeSegmentIfNeeded(value);
            return true;
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private boolean startUnloadAsync(final EvictionCandidate<K, V> candidate) {
        try {
            unloadExecutor.execute(() -> unloadAndFinalize(candidate, true));
            return true;
        } catch (final RejectedExecutionException ex) {
            candidate.entry.cancelUnload();
            return false;
        }
    }

    private boolean unloadAndFinalize(final EvictionCandidate<K, V> candidate,
            final boolean eviction) {
        if (!unloadValue(candidate.value)) {
            candidate.entry.cancelUnload();
            return false;
        }
        return finalizeRemoval(candidate.key, candidate.entry, eviction);
    }

    private boolean finalizeRemoval(final SegmentId key,
            final SegmentRegistryEntry<K, V> entry,
            final boolean eviction) {
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

    private static final class EvictionCandidate<K, V> {
        private final SegmentId key;
        private final SegmentRegistryEntry<K, V> entry;
        private final Segment<K, V> value;

        private EvictionCandidate(final SegmentId key,
                final SegmentRegistryEntry<K, V> entry,
                final Segment<K, V> value) {
            this.key = key;
            this.entry = entry;
            this.value = value;
        }
    }

}
