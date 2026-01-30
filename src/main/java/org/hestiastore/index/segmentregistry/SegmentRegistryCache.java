package org.hestiastore.index.segmentregistry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final LinkedHashMap<SegmentId, Segment<K, V>> segments = new LinkedHashMap<>(
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
     * Returns the cached segment instance for the provided id.
     *
     * @param segmentId segment id to look up
     * @return cached segment or null when absent
     */
    public Segment<K, V> getLocked(final SegmentId segmentId) {
        return segments.get(segmentId);
    }

    /**
     * Inserts or replaces a cached segment instance.
     *
     * @param segmentId segment id to cache
     * @param segment   segment instance
     */
    public void putLocked(final SegmentId segmentId,
            final Segment<K, V> segment) {
        segments.put(segmentId, segment);
    }

    /**
     * Removes the cached segment instance for the provided id.
     *
     * @param segmentId segment id to remove
     * @return removed segment or null when missing
     */
    public Segment<K, V> removeLocked(final SegmentId segmentId) {
        return segments.remove(segmentId);
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
        return segments.get(segmentId) == expected;
    }

    /**
     * Returns a snapshot of cached segments and clears the cache.
     *
     * @return snapshot list (may be empty)
     */
    public List<Segment<K, V>> snapshotAndClearLocked() {
        if (segments.isEmpty()) {
            return List.of();
        }
        final List<Segment<K, V>> snapshot = new ArrayList<>(segments.values());
        segments.clear();
        return snapshot;
    }

    /**
     * Determines whether eviction is needed, ignoring protected ids.
     *
     * @param maxSegments  max allowed cache size
     * @param protectedIds ids that must not be evicted
     * @return true when eviction is required
     */
    public boolean needsEvictionLocked(final int maxSegments,
            final Set<SegmentId> protectedIds) {
        if (segments.size() <= maxSegments) {
            return false;
        }
        for (final SegmentId segmentId : segments.keySet()) {
            if (!protectedIds.contains(segmentId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Evicts least-recently-used segments until the cache fits.
     *
     * @param maxSegments  max allowed cache size
     * @param protectedIds ids that must not be evicted
     * @param evicted      output list of evicted segments
     */
    public void evictIfNeededLocked(final int maxSegments,
            final Set<SegmentId> protectedIds,
            final List<Segment<K, V>> evicted) {
        if (segments.size() <= maxSegments) {
            return;
        }
        final var iterator = segments.entrySet().iterator();
        while (segments.size() > maxSegments && iterator.hasNext()) {
            final Map.Entry<SegmentId, Segment<K, V>> eldest = iterator.next();
            if (protectedIds.contains(eldest.getKey())) {
                continue;
            }
            iterator.remove();
            evicted.add(eldest.getValue());
        }
    }
}
