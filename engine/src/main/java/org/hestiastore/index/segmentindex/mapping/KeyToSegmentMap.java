package org.hestiastore.index.segmentindex.mapping;

import java.util.List;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.split.RouteSplitPlan;

/**
 * Holds {@code map<key, segmentId>} where each key is the max key in a given
 * segment.
 *
 * Provide information about keys and particular segment files. Each key
 * represents one segment. All keys in that segment are equal or smaller than
 * the given key. The last key represents the highest key in the index. When a
 * new value outside current routing is entered, the max key must be extended so
 * routing still covers the whole key space.
 *
 * Note that this is similar to scarce index, but still different.
 *
 * @param <K> key type
 */
public interface KeyToSegmentMap<K> extends CloseableResource {

    void validateUniqueSegmentIds();

    SegmentId findSegmentIdForKey(K key);

    Snapshot<K> snapshot();

    boolean isAtVersion(long expectedVersion);

    boolean isSnapshotVersionCurrent(long expectedVersion);

    boolean extendMaxKeyIfNeeded(K key);

    boolean tryApplySplitPlan(RouteSplitPlan<K> plan);

    void removeSegmentRoute(SegmentId segmentId);

    List<SegmentId> getSegmentIds();

    List<SegmentId> getSegmentIds(SegmentWindow segmentWindow);

    void flushIfDirty();
}
