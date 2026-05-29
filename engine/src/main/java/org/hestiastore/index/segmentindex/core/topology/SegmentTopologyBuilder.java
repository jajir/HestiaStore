package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.mapping.Snapshot;

/**
 * Builder for the default segment topology implementation.
 *
 * @param <K> key type
 */
public final class SegmentTopologyBuilder<K> {

    private Snapshot<K> snapshot;
    private BusyRetryPolicy retryPolicy;

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
     * Sets the retry policy used while waiting for route leases to drain.
     *
     * @param retryPolicy retry policy
     * @return this builder
     */
    public SegmentTopologyBuilder<K> retryPolicy(
            final BusyRetryPolicy retryPolicy) {
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        return this;
    }

    /**
     * Builds a segment topology.
     *
     * @return segment topology
     */
    public SegmentTopology<K> build() {
        return new SegmentTopologyImpl<>(
                Vldtn.requireNonNull(snapshot, "snapshot"),
                Vldtn.requireNonNull(retryPolicy, "retryPolicy"));
    }
}
