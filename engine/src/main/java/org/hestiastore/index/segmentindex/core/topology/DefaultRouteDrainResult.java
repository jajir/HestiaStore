package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.Vldtn;

final class DefaultRouteDrainResult
        implements SegmentTopology.RouteDrainResult {

    private final SegmentTopology.RouteDrain drain;

    private DefaultRouteDrainResult(final SegmentTopology.RouteDrain drain) {
        this.drain = drain;
    }

    static SegmentTopology.RouteDrainResult acquired(
            final SegmentTopology.RouteDrain drain) {
        return new DefaultRouteDrainResult(
                Vldtn.requireNonNull(drain, "drain"));
    }

    static SegmentTopology.RouteDrainResult routeUnavailable() {
        return new DefaultRouteDrainResult(null);
    }

    @Override
    public boolean isAcquired() {
        return drain != null;
    }

    @Override
    public SegmentTopology.RouteDrain drain() {
        if (!isAcquired()) {
            throw new IllegalStateException("Route drain is not available.");
        }
        return drain;
    }
}
