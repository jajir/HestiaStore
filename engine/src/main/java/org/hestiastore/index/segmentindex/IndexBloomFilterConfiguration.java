package org.hestiastore.index.segmentindex;

/**
 * Immutable Bloom filter settings view.
 */
public final class IndexBloomFilterConfiguration {

    private final Integer hashFunctions;
    private final Integer indexSizeBytes;
    private final Double falsePositiveProbability;

    public IndexBloomFilterConfiguration(final Integer hashFunctions,
            final Integer indexSizeBytes,
            final Double falsePositiveProbability) {
        this.hashFunctions = hashFunctions;
        this.indexSizeBytes = indexSizeBytes;
        this.falsePositiveProbability = falsePositiveProbability;
    }

    public Integer hashFunctions() {
        return hashFunctions;
    }

    public Integer indexSizeBytes() {
        return indexSizeBytes;
    }

    public Double falsePositiveProbability() {
        return falsePositiveProbability;
    }
}
