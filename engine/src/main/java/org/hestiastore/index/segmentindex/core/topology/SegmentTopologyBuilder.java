package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.mapping.Snapshot;

/**
 * Builder for the default segment topology implementation.
 *
 * @param <K> key type
 */
public final class SegmentTopologyBuilder<K> {

    private Snapshot<K> snapshot;

    SegmentTopologyBuilder() {
    }

    /**
     * Sets the initial route-map snapshot.
     *
     * @param snapshot initial route-map snapshot
     * @return this builder
     */
    public SegmentTopologyBuilder<K> snapshot(final Snapshot<K> snapshot) {
        this.snapshot = Vldtn.requireNonNull(snapshot, "snapshot");
        return this;
    }

    /**
     * Builds a segment topology.
     *
     * @return segment topology
     */
    public SegmentTopology<K> build() {
        return new DefaultSegmentTopology<>(
                Vldtn.requireNonNull(snapshot, "snapshot"));
    }
}
