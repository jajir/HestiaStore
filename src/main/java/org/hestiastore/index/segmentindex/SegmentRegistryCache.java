package org.hestiastore.index.segmentindex;

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
final class SegmentRegistryCache<K, V> {

    private final LinkedHashMap<SegmentId, Segment<K, V>> segments = new LinkedHashMap<>(
            16, 0.75f, true);
    private final Object lock = new Object();

    <T> T withLock(final Supplier<T> action) {
        synchronized (lock) {
            return action.get();
        }
    }

    void withLock(final Runnable action) {
        synchronized (lock) {
            action.run();
        }
    }

    Segment<K, V> getLocked(final SegmentId segmentId) {
        return segments.get(segmentId);
    }

    void putLocked(final SegmentId segmentId, final Segment<K, V> segment) {
        segments.put(segmentId, segment);
    }

    Segment<K, V> removeLocked(final SegmentId segmentId) {
        return segments.remove(segmentId);
    }

    boolean isSegmentInstanceLocked(final SegmentId segmentId,
            final Segment<K, V> expected) {
        return segments.get(segmentId) == expected;
    }

    List<Segment<K, V>> snapshotAndClearLocked() {
        if (segments.isEmpty()) {
            return List.of();
        }
        final List<Segment<K, V>> snapshot = new ArrayList<>(segments.values());
        segments.clear();
        return snapshot;
    }

    boolean needsEvictionLocked(final int maxSegments,
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

    void evictIfNeededLocked(final int maxSegments,
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
