package org.hestiastore.index.segmentindex.core.segmentaccess;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for {@link SegmentAccessService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentAccessServiceBuilder<K, V> {

    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private SegmentTopology<K> segmentTopology;
    private IndexRetryPolicy retryPolicy;

    SegmentAccessServiceBuilder() {
    }

    /**
     * Sets the key-to-segment map used for resolving keys.
     *
     * @param keyToSegmentMap key-to-segment map
     * @return this builder
     */
    public SegmentAccessServiceBuilder<K, V> keyToSegmentMap(
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
    public SegmentAccessServiceBuilder<K, V> segmentRegistry(
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
    public SegmentAccessServiceBuilder<K, V> segmentTopology(
            final SegmentTopology<K> segmentTopology) {
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        return this;
    }

    /**
     * Sets the retry policy used while waiting for route access.
     *
     * @param retryPolicy retry policy
     * @return this builder
     */
    public SegmentAccessServiceBuilder<K, V> retryPolicy(
            final IndexRetryPolicy retryPolicy) {
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        return this;
    }

    /**
     * Builds the service.
     *
     * @return segment access service
     */
    public SegmentAccessService<K, V> build() {
        return new DefaultSegmentAccessService<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(segmentTopology, "segmentTopology"),
                Vldtn.requireNonNull(retryPolicy, "retryPolicy"));
    }
}
