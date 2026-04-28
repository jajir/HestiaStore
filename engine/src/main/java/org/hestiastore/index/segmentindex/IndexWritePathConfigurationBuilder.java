package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for direct-to-segment write-path limits.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexWritePathConfigurationBuilder<K, V> {

    private final IndexConfigurationBuilder<K, V> builder;

    IndexWritePathConfigurationBuilder(
            final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Sets steady-state segment write-cache key limit.
     *
     * @param value segment write-cache key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> segmentWriteCacheKeyLimit(
            final Integer value) {
        builder.setSegmentWriteCacheKeyLimit(value);
        return this;
    }

    /**
     * Sets maintenance-time segment write-cache key limit.
     *
     * @param value maintenance write-cache key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> maintenanceWriteCacheKeyLimit(
            final Integer value) {
        builder.setSegmentWriteCacheKeyLimitDuringMaintenance(value);
        return this;
    }

    /**
     * Sets index-wide buffered write key limit.
     *
     * @param value index-wide buffered key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> indexBufferedWriteKeyLimit(
            final Integer value) {
        builder.setIndexBufferedWriteKeyLimit(value);
        return this;
    }

    /**
     * Sets segment split key threshold.
     *
     * @param value segment split threshold
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> segmentSplitKeyThreshold(
            final Integer value) {
        builder.setSegmentSplitKeyThreshold(value);
        return this;
    }

    /**
     * Sets the compatibility limit used when loading older manifests.
     *
     * @param value immutable run limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder<K, V> legacyImmutableRunLimit(
            final Integer value) {
        builder.setLegacyImmutableRunLimit(value);
        return this;
    }
}
