package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology.RouteDrain;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Scoped exclusive lease for split work against one drained segment route.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class RouteSplitLease<K, V> implements AutoCloseable {

    private final RouteDrain routeDrain;
    private final SegmentRouteMap<K> keyToSegmentMap;
    private final RouteTopology<K> segmentTopology;
    private final BlockingSegment<K, V> segment;
    private boolean finished;

    RouteSplitLease(final RouteDrain routeDrain,
            final SegmentRouteMap<K> keyToSegmentMap,
            final RouteTopology<K> segmentTopology,
            final BlockingSegment<K, V> segment) {
        this.routeDrain = Vldtn.requireNonNull(routeDrain, "routeDrain");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        this.segment = Vldtn.requireNonNull(segment, "segment");
    }

    /**
     * Returns the drained segment id.
     *
     * @return segment id
     */
    public SegmentId segmentId() {
        return segment.getId();
    }

    /**
     * Returns the loaded blocking segment.
     *
     * @return blocking segment
     */
    public BlockingSegment<K, V> segment() {
        return segment;
    }

    /**
     * Completes the drain after the split route was published.
     */
    public void completeAfterPublish() {
        if (finished) {
            return;
        }
        RuntimeException reconcileFailure = null;
        try {
            segmentTopology.reconcile(keyToSegmentMap.snapshot());
        } catch (final RuntimeException e) {
            reconcileFailure = e;
        } finally {
            routeDrain.complete();
            finished = true;
        }
        if (reconcileFailure != null) {
            throw reconcileFailure;
        }
    }

    /**
     * Aborts the drain and returns the route to active state.
     */
    public void abort() {
        if (finished) {
            return;
        }
        try {
            routeDrain.abort();
        } finally {
            finished = true;
        }
    }

    /**
     * Aborts the drain when the lease is closed before successful publish.
     */
    @Override
    public void close() {
        abort();
    }
}
