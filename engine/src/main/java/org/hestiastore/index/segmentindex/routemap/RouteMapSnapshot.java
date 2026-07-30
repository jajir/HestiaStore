package org.hestiastore.index.segmentindex.routemap;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Immutable point-in-time view of a {@link PersistentSegmentRouteMap} routing
 * topology.
 *
 * @param <K> key type
 */
public final class RouteMapSnapshot<K> {

    private final TreeMap<K, SegmentId> map;
    private final long version;

    RouteMapSnapshot(final TreeMap<K, SegmentId> map, final long version) {
        this.map = Vldtn.requireNonNull(map, "map");
        this.version = version;
    }

    public SegmentId findSegmentIdForKey(final K key) {
        Vldtn.requireNonNull(key, "key");
        final Map.Entry<K, SegmentId> ceilingEntry = map.ceilingEntry(key);
        if (ceilingEntry != null) {
            return ceilingEntry.getValue();
        }
        final Map.Entry<K, SegmentId> tailEntry = map.lastEntry();
        return tailEntry == null ? null : tailEntry.getValue();
    }

    public List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
        Vldtn.requireNonNull(segmentWindow, "segmentWindow");
        return map.entrySet().stream()//
                .skip(segmentWindow.getIntOffset())//
                .limit(segmentWindow.getIntLimit())//
                .map(entry -> entry.getValue())//
                .toList();
    }

    /**
     * Returns segment ids whose routed key intervals intersect the requested
     * half-open key range.
     *
     * @param fromInclusive required inclusive lower key bound
     * @param toExclusive optional exclusive upper key bound; {@code null}
     *        selects through the tail segment
     * @return ordered intersecting segment ids
     */
    public List<SegmentId> getSegmentIds(final K fromInclusive,
            final K toExclusive) {
        final K lowerBound = Vldtn.requireNonNull(fromInclusive,
                "fromInclusive");
        if (map.isEmpty()) {
            return List.of();
        }
        final Map.Entry<K, SegmentId> first = segmentForKey(lowerBound);
        if (toExclusive == null) {
            return List.copyOf(
                    map.tailMap(first.getKey(), true).values());
        }
        final Map.Entry<K, SegmentId> last = segmentForKey(toExclusive);
        final NavigableMap<K, SegmentId> selected = map.subMap(first.getKey(),
                true, last.getKey(), true);
        return List.copyOf(selected.values());
    }

    private Map.Entry<K, SegmentId> segmentForKey(final K key) {
        final Map.Entry<K, SegmentId> ceilingEntry = map.ceilingEntry(key);
        return ceilingEntry == null ? map.lastEntry() : ceilingEntry;
    }

    /**
     * Returns whether this snapshot contains the provided segment id.
     *
     * @param segmentId segment id to find
     * @return {@code true} when the segment id is routed
     */
    public boolean containsSegmentId(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return map.containsValue(segmentId);
    }

    public long version() {
        return version;
    }
}
