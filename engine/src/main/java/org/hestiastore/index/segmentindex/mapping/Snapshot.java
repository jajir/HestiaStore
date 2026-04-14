package org.hestiastore.index.segmentindex.mapping;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Immutable point-in-time view of a {@link KeyToSegmentMapImpl} routing
 * topology.
 *
 * @param <K> key type
 */
public final class Snapshot<K> {

    private final TreeMap<K, SegmentId> map;
    private final long version;

    Snapshot(final TreeMap<K, SegmentId> map, final long version) {
        this.map = Vldtn.requireNonNull(map, "map");
        this.version = version;
    }

    public SegmentId findSegmentIdForKey(final K key) {
        Vldtn.requireNonNull(key, "key");
        final Map.Entry<K, SegmentId> ceilingEntry = map.ceilingEntry(key);
        return ceilingEntry == null ? null : ceilingEntry.getValue();
    }

    public List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
        Vldtn.requireNonNull(segmentWindow, "segmentWindow");
        return map.entrySet().stream()//
                .skip(segmentWindow.getIntOffset())//
                .limit(segmentWindow.getIntLimit())//
                .map(entry -> entry.getValue())//
                .toList();
    }

    public long version() {
        return version;
    }
}
