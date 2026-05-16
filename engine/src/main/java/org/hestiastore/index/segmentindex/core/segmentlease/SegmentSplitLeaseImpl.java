package org.hestiastore.index.segmentindex.core.segmentlease;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteDrain;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;

final class SegmentSplitLeaseImpl<K, V> implements SegmentSplitLease<K, V> {

    private final RouteDrain routeDrain;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentTopology<K> segmentTopology;
    private final BlockingSegment<K, V> segment;
    private boolean finished;

    SegmentSplitLeaseImpl(final RouteDrain routeDrain,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentTopology<K> segmentTopology,
            final BlockingSegment<K, V> segment) {
        this.routeDrain = Vldtn.requireNonNull(routeDrain, "routeDrain");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        this.segment = Vldtn.requireNonNull(segment, "segment");
    }

    @Override
    public SegmentId segmentId() {
        return segment.getId();
    }

    @Override
    public BlockingSegment<K, V> segment() {
        return segment;
    }

    @Override
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

    @Override
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

    @Override
    public void close() {
        abort();
    }
}
