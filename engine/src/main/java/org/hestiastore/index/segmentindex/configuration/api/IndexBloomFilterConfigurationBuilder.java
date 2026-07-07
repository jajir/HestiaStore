package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Builder section for Bloom filter settings.
 */
public final class IndexBloomFilterConfigurationBuilder {

    private Integer hashFunctions;
    private Integer indexSizeBytes;
    private Double falsePositiveProbability;

    IndexBloomFilterConfigurationBuilder() {
    }

    /**
     * Sets Bloom filter hash function count.
     *
     * @param value hash function count
     * @return this section builder
     */
    public IndexBloomFilterConfigurationBuilder hashFunctions(
            final Integer value) {
        this.hashFunctions = value;
        return this;
    }

    /**
     * Sets Bloom filter index size in bytes.
     *
     * @param value index size in bytes
     * @return this section builder
     */
    public IndexBloomFilterConfigurationBuilder indexSizeBytes(
            final Integer value) {
        this.indexSizeBytes = value;
        return this;
    }

    /**
     * Sets Bloom filter target false-positive probability.
     *
     * @param value false-positive probability
     * @return this section builder
     */
    public IndexBloomFilterConfigurationBuilder falsePositiveProbability(
            final Double value) {
        this.falsePositiveProbability = value;
        return this;
    }

    /**
     * Disables Bloom filter index allocation.
     *
     * @return this section builder
     */
    public IndexBloomFilterConfigurationBuilder disabled() {
        this.indexSizeBytes = Integer.valueOf(0);
        return this;
    }

    IndexBloomFilterConfiguration build() {
        return new IndexBloomFilterConfiguration(hashFunctions, indexSizeBytes,
                falsePositiveProbability);
    }
}
