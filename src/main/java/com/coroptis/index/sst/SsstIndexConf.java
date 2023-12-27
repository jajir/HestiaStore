package com.coroptis.index.sst;

public class SsstIndexConf {

    /*
     * Segments configuration
     */

    private final long maxNumberOfKeysInSegmentCache;
    private final int maxNumberOfKeysInSegmentIndexPage;

    /*
     * SST index configuration
     */

    private final int maxNumberOfKeysInCache;
    private final int maxNumberOfKeysInSegment;
    private final int maxNumberOfSegmentsInCache;

    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;

    SsstIndexConf(final long maxNumberOfKeysInSegmentCache,
            final int maxNumberOfKeysInSegmentIndexPage,
            final int maxNumberOfKeysInCache,
            final int maxNumberOfKeysInSegment,
            final int maxNumberOfSegmentsInCache,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
    }

    public long getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    public int getMaxNumberOfKeysInSegmentIndexPage() {
        return maxNumberOfKeysInSegmentIndexPage;
    }

    public long getMaxNumberOfKeysInCache() {
        return maxNumberOfKeysInCache;
    }

    public int getMaxNumberOfKeysInSegment() {
        return maxNumberOfKeysInSegment;
    }

    public int getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    public int getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    public int getMaxNumberOfSegmentsInCache() {
        return maxNumberOfSegmentsInCache;
    }

}
