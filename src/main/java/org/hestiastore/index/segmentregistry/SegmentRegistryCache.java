package org.hestiastore.index.segmentregistry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * LRU segment cache with a registry-owned lock.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentRegistryCache<K, V> {

    private final LinkedHashMap<SegmentId, SegmentHandler<K, V>> handlers = new LinkedHashMap<>(
            16, 0.75f, true);
    private final Object lock = new Object();

    /**
     * Executes the supplied action under the cache lock.
     *
     * @param action action to execute
     * @param <T>    return type
     * @return action result
     */
    public <T> T withLock(final Supplier<T> action) {
        synchronized (lock) {
            return action.get();
        }
    }

    /**
     * Executes the supplied action under the cache lock.
     *
     * @param action action to execute
     */
    public void withLock(final Runnable action) {
        synchronized (lock) {
            action.run();
        }
    }

    /**
     * Returns the cached handler instance for the provided id.
     *
     * @param segmentId segment id to look up
     * @return cached handler or null when absent
     */
    public SegmentHandler<K, V> getLocked(final SegmentId segmentId) {
        return handlers.get(segmentId);
    }

    /**
     * Inserts or replaces a cached handler instance.
     *
     * @param segmentId segment id to cache
     * @param handler   handler instance
     */
    public void putLocked(final SegmentId segmentId,
            final SegmentHandler<K, V> handler) {
        handlers.put(segmentId, handler);
    }

    /**
     * Removes the cached handler instance for the provided id.
     *
     * @param segmentId segment id to remove
     * @return removed handler or null when missing
     */
    public SegmentHandler<K, V> removeLocked(final SegmentId segmentId) {
        return handlers.remove(segmentId);
    }

    /**
     * Returns true when the cache still maps the id to the expected instance.
     *
     * @param segmentId segment id to check
     * @param expected  expected segment instance
     * @return true when the instance matches
     */
    public boolean isSegmentInstanceLocked(final SegmentId segmentId,
            final Segment<K, V> expected) {
        final SegmentHandler<K, V> handler = handlers.get(segmentId);
        return handler != null && handler.isForSegment(expected);
    }

    /**
     * Returns a snapshot of cached segments and clears the cache.
     *
     * @return snapshot list (may be empty)
     */
    public List<Segment<K, V>> snapshotAndClearLocked() {
        if (handlers.isEmpty()) {
            return List.of();
        }
        final List<Segment<K, V>> snapshot = new ArrayList<>(handlers.size());
        for (final SegmentHandler<K, V> handler : handlers.values()) {
            snapshot.add(handler.getSegment());
        }
        handlers.clear();
        return snapshot;
    }

    /**
     * Determines whether eviction is needed, skipping locked handlers.
     *
     * @param maxSegments max allowed cache size
     * @return true when eviction is required
     */
    public boolean needsEvictionLocked(final int maxSegments) {
        if (handlers.size() <= maxSegments) {
            return false;
        }
        for (final SegmentHandler<K, V> handler : handlers.values()) {
            if (handler.getState() != SegmentHandlerState.LOCKED) {
                return true;
            }
        }
        return false;
    }

    /**
     * Evicts least-recently-used segments until the cache fits, skipping
     * locked handlers.
     *
     * @param maxSegments max allowed cache size
     * @param evicted     output list of evicted segments
     */
    public void evictIfNeededLocked(final int maxSegments,
            final List<Segment<K, V>> evicted) {
        if (handlers.size() <= maxSegments) {
            return;
        }
        final var iterator = handlers.entrySet().iterator();
        while (handlers.size() > maxSegments && iterator.hasNext()) {
            final Map.Entry<SegmentId, SegmentHandler<K, V>> eldest = iterator
                    .next();
            if (eldest.getValue()
                    .getState() == SegmentHandlerState.LOCKED) {
                continue;
            }
            iterator.remove();
            evicted.add(eldest.getValue().getSegment());
        }
    }
}
