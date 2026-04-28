package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for Bloom filter settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexBloomFilterConfigurationBuilder<K, V> {

    private final IndexConfigurationBuilder<K, V> builder;

    IndexBloomFilterConfigurationBuilder(
            final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Sets Bloom filter hash function count.
     *
     * @param value hash function count
     * @return this section builder
     */
    public IndexBloomFilterConfigurationBuilder<K, V> hashFunctions(
            final Integer value) {
        builder.setBloomFilterHashFunctionCount(value);
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
        builder.setBloomFilterIndexSizeBytes(value);
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
        builder.setBloomFilterFalsePositiveProbability(value);
        return this;
    }

    /**
     * Disables Bloom filter index allocation.
     *
     * @return this section builder
     */
    public IndexBloomFilterConfigurationBuilder<K, V> disabled() {
        builder.setBloomFilterIndexSizeBytes(Integer.valueOf(0));
        return this;
    }
}
