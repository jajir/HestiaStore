package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved Bloom filter settings.
 */
public final class EffectiveIndexBloomFilterConfiguration {

    private final int hashFunctions;
    private final int indexSizeBytes;
    private final double falsePositiveProbability;

    public EffectiveIndexBloomFilterConfiguration(final int hashFunctions,
            final int indexSizeBytes, final double falsePositiveProbability) {
        this.hashFunctions = Vldtn.requireGreaterThanZero(hashFunctions,
                "hashFunctions");
        this.indexSizeBytes = Vldtn.requireGreaterThanOrEqualToZero(
                indexSizeBytes, "indexSizeBytes");
        Vldtn.requireTrue(falsePositiveProbability > 0.0d
                && falsePositiveProbability < 1.0d,
                "falsePositiveProbability must be greater than 0 and less than 1");
        this.falsePositiveProbability = falsePositiveProbability;
    }

    public int hashFunctions() {
        return hashFunctions;
    }

    public int indexSizeBytes() {
        return indexSizeBytes;
    }

    public double falsePositiveProbability() {
        return falsePositiveProbability;
    }
}
