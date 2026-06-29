package org.hestiastore.index.segmentindex.routemap;

import java.util.List;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Holds {@code map<key, segmentId>} where each key is a route boundary.
 *
 * Provide information about keys and particular segment files. Each key
 * represents one segment. Non-tail route keys are inclusive upper bounds. The
 * last route is an open-ended tail, so keys above the last stored boundary
 * still route to the last segment.
 *
 * Note that this is similar to scarce index, but still different.
 *
 * @param <K> key type
 */
public interface SegmentRouteMap<K> extends CloseableResource {

    void validateUniqueSegmentIds();

    SegmentId findSegmentIdForKey(K key);

    RouteMapSnapshot<K> snapshot();

    boolean isAtVersion(long expectedVersion);

    void extendMaxKeyIfNeeded(K key);

    boolean tryReplaceRouteWithSplit(RouteSplitPlan<K> split);

    void removeSegmentRoute(SegmentId segmentId);

    List<SegmentId> getSegmentIds();

    List<SegmentId> getSegmentIds(SegmentWindow segmentWindow);

    void flushIfDirty();
}
