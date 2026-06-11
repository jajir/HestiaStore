package org.hestiastore.index.segmentindex.runtimemonitoring.model;

/**
 * User-facing Bloom filter metrics.
 */
public final class SegmentIndexBloomFilterMetrics {

    private final int hashFunctions;
    private final int indexSizeInBytes;
    private final double probabilityOfFalsePositive;
    private final long requestCount;
    private final long refusedCount;
    private final long positiveCount;
    private final long falsePositiveCount;

    /**
     * Creates Bloom filter metrics.
     *
     * @param hashFunctions configured hash function count
     * @param indexSizeInBytes configured Bloom filter index size
     * @param probabilityOfFalsePositive configured false-positive probability
     * @param requestCount request count
     * @param refusedCount refused count
     * @param positiveCount positive count
     * @param falsePositiveCount false-positive count
     */
    public SegmentIndexBloomFilterMetrics(final int hashFunctions,
            final int indexSizeInBytes,
            final double probabilityOfFalsePositive,
            final long requestCount, final long refusedCount,
            final long positiveCount, final long falsePositiveCount) {
        this.hashFunctions = MetricModelValidation.nonNegative(hashFunctions,
                "hashFunctions");
        this.indexSizeInBytes = MetricModelValidation.nonNegative(
                indexSizeInBytes, "indexSizeInBytes");
        if (probabilityOfFalsePositive < 0D) {
            throw new IllegalArgumentException(
                    "probabilityOfFalsePositive must be >= 0");
        }
        this.probabilityOfFalsePositive = probabilityOfFalsePositive;
        this.requestCount = MetricModelValidation.nonNegative(requestCount,
                "requestCount");
        this.refusedCount = MetricModelValidation.nonNegative(refusedCount,
                "refusedCount");
        this.positiveCount = MetricModelValidation.nonNegative(positiveCount,
                "positiveCount");
        this.falsePositiveCount = MetricModelValidation.nonNegative(
                falsePositiveCount, "falsePositiveCount");
    }

    /**
     * Returns configured hash function count.
     *
     * @return hash function count
     */
    public int hashFunctions() {
        return hashFunctions;
    }

    /**
     * Returns configured Bloom filter index size.
     *
     * @return index size in bytes
     */
    public int indexSizeInBytes() {
        return indexSizeInBytes;
    }

    /**
     * Returns configured false-positive probability.
     *
     * @return false-positive probability
     */
    public double probabilityOfFalsePositive() {
        return probabilityOfFalsePositive;
    }

    /**
     * Returns request count.
     *
     * @return request count
     */
    public long requestCount() {
        return requestCount;
    }

    /**
     * Returns refused count.
     *
     * @return refused count
     */
    public long refusedCount() {
        return refusedCount;
    }

    /**
     * Returns positive count.
     *
     * @return positive count
     */
    public long positiveCount() {
        return positiveCount;
    }

    /**
     * Returns false-positive count.
     *
     * @return false-positive count
     */
    public long falsePositiveCount() {
        return falsePositiveCount;
    }
}
