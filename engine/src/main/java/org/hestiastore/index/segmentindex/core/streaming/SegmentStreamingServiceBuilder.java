package org.hestiastore.index.segmentindex.core.streaming;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Builder for {@link SegmentStreamingService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentStreamingServiceBuilder<K, V> {

    private Logger logger;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private StableSegmentOperationAccess<K, V> stableSegmentGateway;
    private IndexRetryPolicy retryPolicy;

    SegmentStreamingServiceBuilder() {
    }

    /**
     * Sets the logger used by streaming operations.
     *
     * @param logger logger
     * @return this builder
     */
    public SegmentStreamingServiceBuilder<K, V> logger(final Logger logger) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        return this;
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
            final StableSegmentOperationAccess<K, V> stableSegmentGateway) {
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        return this;
    }

    /**
     * Sets the retry policy used for transient busy states.
     *
     * @param retryPolicy retry policy
     * @return this builder
     */
    public SegmentStreamingServiceBuilder<K, V> retryPolicy(
            final IndexRetryPolicy retryPolicy) {
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        return this;
    }

    /**
     * Builds the service.
     *
     * @return segment streaming service
     */
    public SegmentStreamingService<K, V> build() {
        return new SegmentStreamingServiceImpl<>(
                Vldtn.requireNonNull(logger, "logger"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(stableSegmentGateway,
                        "stableSegmentGateway"),
                Vldtn.requireNonNull(retryPolicy, "retryPolicy"));
    }
}
