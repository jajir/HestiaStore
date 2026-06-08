package org.hestiastore.index.segmentindex.core.segmentlease;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for {@link SegmentLeaseService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentLeaseServiceBuilder<K, V> {

    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private SegmentTopology<K> segmentTopology;
    private Integer busyBackoffMillis;
    private Integer busyTimeoutMillis;

    SegmentLeaseServiceBuilder() {
    }

    /**
     * Sets the key-to-segment map used for resolving keys.
     *
     * @param keyToSegmentMap key-to-segment map
     * @return this builder
     */
    public SegmentLeaseServiceBuilder<K, V> keyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        return this;
    }

    /**
     * Sets the segment registry used for loading blocking segments.
     *
     * @param segmentRegistry segment registry
     * @return this builder
     */
    public SegmentLeaseServiceBuilder<K, V> segmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        return this;
    }

    /**
     * Sets the topology used for route leasing and draining coordination.
     *
     * @param segmentTopology segment topology
     * @return this builder
     */
    public SegmentLeaseServiceBuilder<K, V> segmentTopology(
            final SegmentTopology<K> segmentTopology) {
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        return this;
    }

    /**
     * Sets the backoff value used to create the package-local segment access
     * retry policy.
     *
     * @param busyBackoffMillis backoff in milliseconds
     * @return this builder
     */
    public SegmentLeaseServiceBuilder<K, V> busyBackoffMillis(
            final int busyBackoffMillis) {
        this.busyBackoffMillis = busyBackoffMillis;
        return this;
    }

    /**
     * Sets the timeout value used to create the package-local segment access
     * retry policy.
     *
     * @param busyTimeoutMillis timeout in milliseconds
     * @return this builder
     */
    public SegmentLeaseServiceBuilder<K, V> busyTimeoutMillis(
            final int busyTimeoutMillis) {
        this.busyTimeoutMillis = busyTimeoutMillis;
        return this;
    }

    /**
     * Builds the service.
     *
     * @return segment lease service
     */
    public SegmentLeaseService<K, V> build() {
        return new SegmentLeaseServiceImpl<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(segmentTopology, "segmentTopology"),
                new SegmentAccessRetryPolicy(Vldtn.requireNonNull(
                        busyBackoffMillis, "busyBackoffMillis"),
                        Vldtn.requireNonNull(busyTimeoutMillis,
                                "busyTimeoutMillis")));
    }
}
