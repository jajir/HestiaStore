package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Builder section for direct-to-segment write-path limits.
 */
public final class IndexWritePathConfigurationBuilder {

    private Integer segmentWriteCacheKeyLimit;
    private Integer maintenanceWriteCacheKeyLimit;
    private Integer indexBufferedWriteKeyLimit;
    private Integer segmentSplitKeyThreshold;

    IndexWritePathConfigurationBuilder() {
    }

    /**
     * Sets steady-state segment write-cache key limit.
     *
     * @param value segment write-cache key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder segmentWriteCacheKeyLimit(
            final Integer value) {
        this.segmentWriteCacheKeyLimit = value;
        return this;
    }

    /**
     * Sets maintenance-time segment write-cache key limit.
     *
     * @param value maintenance write-cache key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder maintenanceWriteCacheKeyLimit(
            final Integer value) {
        this.maintenanceWriteCacheKeyLimit = value;
        return this;
    }

    /**
     * Sets index-wide buffered write key limit.
     *
     * @param value index-wide buffered key limit
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder indexBufferedWriteKeyLimit(
            final Integer value) {
        this.indexBufferedWriteKeyLimit = value;
        return this;
    }

    /**
     * Sets segment split key threshold.
     *
     * @param value segment split threshold
     * @return this section builder
     */
    public IndexWritePathConfigurationBuilder segmentSplitKeyThreshold(
            final Integer value) {
        this.segmentSplitKeyThreshold = value;
        return this;
    }

    IndexWritePathConfiguration build() {
        return new IndexWritePathConfiguration(segmentWriteCacheKeyLimit,
                maintenanceWriteCacheKeyLimit, indexBufferedWriteKeyLimit,
                segmentSplitKeyThreshold);
    }

}
