package com.coroptis.index.segment;

public class SegmentConf {

    private final long maxNumberOfKeysInSegmentCache;
    private final long maxNumberOfKeysInSegmentCacheDuringFlushing;
    private final int maxNumberOfKeysInIndexPage;
    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;

    private final long maxNumberOfKeysInSegmentMemory;

    public SegmentConf(final long maxNumeberOfKeysInSegmentCache,
            final long maxNumberOfKeysInSegmentCacheDuringFlushing,
            final int maxNumberOfKeysInIndexPage,
            final Integer bloomFilterNumberOfHashFunctions,
            final Integer bloomFilterIndexSizeInBytes,
            final Double bloomFilterProbabilityOfFalsePositive,
            final long maxNumberOfKeysInSegmentMemory) {
        this.maxNumberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.maxNumberOfKeysInSegmentMemory = maxNumberOfKeysInSegmentMemory;
    }

    long getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    long getMaxNumberOfKeysInSegmentMemory() {
        return maxNumberOfKeysInSegmentMemory;
    }

    Integer getMaxNumberOfKeysInIndexPage() {
        return maxNumberOfKeysInIndexPage;
    }

    Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return maxNumberOfKeysInSegmentCacheDuringFlushing;
    }
}
