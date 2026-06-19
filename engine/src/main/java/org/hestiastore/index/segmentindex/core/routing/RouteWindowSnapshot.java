package org.hestiastore.index.segmentindex.core.routing;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Versioned routed segment ids for one segment window.
 */
public final class RouteWindowSnapshot {

    private final List<SegmentId> segmentIds;
    private final long version;

    /**
     * Creates a versioned segment-window snapshot.
     *
     * @param segmentIds routed segment ids
     * @param version route-map version
     */
    public RouteWindowSnapshot(final List<SegmentId> segmentIds,
            final long version) {
        this.segmentIds = List.copyOf(Vldtn.requireNonNull(segmentIds,
                "segmentIds"));
        this.version = version;
    }

    /**
     * Returns routed segment ids captured by this snapshot.
     *
     * @return routed segment ids
     */
    public List<SegmentId> segmentIds() {
        return segmentIds;
    }

    long version() {
        return version;
    }
}
