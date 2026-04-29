package org.hestiastore.index.segmentindex;

/**
 * Builder section for Bloom filter settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexBloomFilterConfigurationBuilder<K, V> {

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
    public IndexBloomFilterConfigurationBuilder<K, V> hashFunctions(
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
    public IndexBloomFilterConfigurationBuilder<K, V> indexSizeBytes(
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
    public IndexBloomFilterConfigurationBuilder<K, V> falsePositiveProbability(
            final Double value) {
        this.falsePositiveProbability = value;
        return this;
    }

    /**
     * Disables Bloom filter index allocation.
     *
     * @return this section builder
     */
    public IndexBloomFilterConfigurationBuilder<K, V> disabled() {
        this.indexSizeBytes = Integer.valueOf(0);
        return this;
    }

    IndexBloomFilterConfiguration build() {
        return new IndexBloomFilterConfiguration(hashFunctions, indexSizeBytes,
                falsePositiveProbability);
    }
}
