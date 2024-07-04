package com.coroptis.index.segment;

public class SegmentConf {

    private final long maxNumberOfKeysInSegmentCache;
    private final long maxNumberOfKeysInSegmentCacheDuringFlushing;
    private final int maxNumberOfKeysInIndexPage;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final long maxNumberOfKeysInSegmentMemory;

    public SegmentConf(final long maxNumeberOfKeysInSegmentCache,
            final long maxNumberOfKeysInSegmentCacheDuringFlushing,
            final int maxNumberOfKeysInIndexPage,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final long maxNumberOfKeysInSegmentMemory) {
        this.maxNumberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.maxNumberOfKeysInSegmentMemory = maxNumberOfKeysInSegmentMemory;
    }

    long getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    long getMaxNumberOfKeysInSegmentMemory() {
        return maxNumberOfKeysInSegmentMemory;
    }

    int getMaxNumberOfKeysInIndexPage() {
        return maxNumberOfKeysInIndexPage;
    }

    int getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    int getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return maxNumberOfKeysInSegmentCacheDuringFlushing;
    }
}
