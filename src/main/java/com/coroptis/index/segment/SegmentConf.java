package com.coroptis.index.segment;

public class SegmentConf {

    private final long maxNumberOfKeysInSegmentCache;
    private final int maxNumberOfKeysInIndexPage;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;

    public SegmentConf(final long maxNumeberOfKeysInSegmentCache,
            final int maxNumberOfKeysInIndexPage,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes) {
        this.maxNumberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
    }

    long getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
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
}
