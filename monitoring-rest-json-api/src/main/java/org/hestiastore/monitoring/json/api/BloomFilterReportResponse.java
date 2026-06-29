package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Bloom filter metrics section inside an index report payload.
 */
public final class BloomFilterReportResponse {

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
     * @param hashFunctions Bloom filter hash function count
     * @param indexSizeInBytes Bloom filter index size
     * @param probabilityOfFalsePositive configured false-positive probability
     * @param requestCount Bloom filter request count
     * @param refusedCount Bloom filter refused count
     * @param positiveCount Bloom filter positive count
     * @param falsePositiveCount Bloom filter false-positive count
     */
    @ConstructorProperties({ "hashFunctions", "indexSizeInBytes",
            "probabilityOfFalsePositive", "requestCount", "refusedCount",
            "positiveCount", "falsePositiveCount" })
    public BloomFilterReportResponse(final int hashFunctions,
            final int indexSizeInBytes,
            final double probabilityOfFalsePositive,
            final long requestCount, final long refusedCount,
            final long positiveCount, final long falsePositiveCount) {
        this.hashFunctions = hashFunctions;
        this.indexSizeInBytes = indexSizeInBytes;
        this.probabilityOfFalsePositive = probabilityOfFalsePositive;
        this.requestCount = requestCount;
        this.refusedCount = refusedCount;
        this.positiveCount = positiveCount;
        this.falsePositiveCount = falsePositiveCount;
    }

    /**
     * Returns Bloom filter hash function count.
     *
     * @return Bloom filter hash function count
     */
    public int hashFunctions() {
        return hashFunctions;
    }

    /**
     * Returns Bloom filter index size.
     *
     * @return Bloom filter index size
     */
    public int indexSizeInBytes() {
        return indexSizeInBytes;
    }

    /**
     * Returns configured false-positive probability.
     *
     * @return configured false-positive probability
     */
    public double probabilityOfFalsePositive() {
        return probabilityOfFalsePositive;
    }

    /**
     * Returns Bloom filter request count.
     *
     * @return Bloom filter request count
     */
    public long requestCount() {
        return requestCount;
    }

    /**
     * Returns Bloom filter refused count.
     *
     * @return Bloom filter refused count
     */
    public long refusedCount() {
        return refusedCount;
    }

    /**
     * Returns Bloom filter positive count.
     *
     * @return Bloom filter positive count
     */
    public long positiveCount() {
        return positiveCount;
    }

    /**
     * Returns Bloom filter false-positive count.
     *
     * @return Bloom filter false-positive count
     */
    public long falsePositiveCount() {
        return falsePositiveCount;
    }
}
