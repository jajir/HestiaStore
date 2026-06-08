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
    private Integer busyBackoffMillis;
    private Integer busyTimeoutMillis;

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
     * Sets the backoff value used to create the package-local route drain
     * retry policy.
     *
     * @param busyBackoffMillis backoff in milliseconds
     * @return this builder
     */
    public SegmentTopologyBuilder<K> busyBackoffMillis(
            final int busyBackoffMillis) {
        this.busyBackoffMillis = busyBackoffMillis;
        return this;
    }

    /**
     * Sets the timeout value used to create the package-local route drain retry
     * policy.
     *
     * @param busyTimeoutMillis timeout in milliseconds
     * @return this builder
     */
    public SegmentTopologyBuilder<K> busyTimeoutMillis(
            final int busyTimeoutMillis) {
        this.busyTimeoutMillis = busyTimeoutMillis;
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
                new RouteDrainRetryPolicy(Vldtn.requireNonNull(
                        busyBackoffMillis, "busyBackoffMillis"),
                        Vldtn.requireNonNull(busyTimeoutMillis,
                                "busyTimeoutMillis")));
    }
}
