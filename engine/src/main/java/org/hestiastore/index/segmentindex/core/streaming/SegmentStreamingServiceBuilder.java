package org.hestiastore.index.segmentindex.core.streaming;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationGateway;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for {@link SegmentStreamingService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentStreamingServiceBuilder<K, V> {

    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private StableSegmentOperationGateway<K, V> stableSegmentGateway;
    private Integer busyBackoffMillis;
    private Integer busyTimeoutMillis;

    SegmentStreamingServiceBuilder() {
    }

    /**
     * Sets the key-to-segment map used for mapped segment scans.
     *
     * @param keyToSegmentMap key-to-segment map
     * @return this builder
     */
    public SegmentStreamingServiceBuilder<K, V> keyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        return this;
    }

    /**
     * Sets the segment registry used for loaded segment iterator invalidation.
     *
     * @param segmentRegistry segment registry
     * @return this builder
     */
    public SegmentStreamingServiceBuilder<K, V> segmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        return this;
    }

    /**
     * Sets the stable segment gateway used to open segment iterators.
     *
     * @param stableSegmentGateway stable segment gateway
     * @return this builder
     */
    public SegmentStreamingServiceBuilder<K, V> stableSegmentGateway(
            final StableSegmentOperationGateway<K, V> stableSegmentGateway) {
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        return this;
    }

    /**
     * Sets the backoff value used to create the package-local streaming retry
     * policy.
     *
     * @param busyBackoffMillis backoff in milliseconds
     * @return this builder
     */
    public SegmentStreamingServiceBuilder<K, V> busyBackoffMillis(
            final int busyBackoffMillis) {
        this.busyBackoffMillis = busyBackoffMillis;
        return this;
    }

    /**
     * Sets the timeout value used to create the package-local streaming retry
     * policy.
     *
     * @param busyTimeoutMillis timeout in milliseconds
     * @return this builder
     */
    public SegmentStreamingServiceBuilder<K, V> busyTimeoutMillis(
            final int busyTimeoutMillis) {
        this.busyTimeoutMillis = busyTimeoutMillis;
        return this;
    }

    /**
     * Builds the service.
     *
     * @return segment streaming service
     */
    public SegmentStreamingService<K, V> build() {
        return new SegmentStreamingService<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(stableSegmentGateway,
                        "stableSegmentGateway"),
                new BusyRetryPolicy(Vldtn.requireNonNull(
                        busyBackoffMillis, "busyBackoffMillis"),
                        Vldtn.requireNonNull(busyTimeoutMillis,
                                "busyTimeoutMillis"),
                        "Streaming operation"));
    }
}
