package com.coroptis.index.sst;

public class SsstIndexConf {

    /*
     * Segments configuration
     */

    private final long maxNumberOfKeysInSegmentCache;
    private final long maxNumberOfKeysInSegmentCacheDuringFlushing;
    private final int maxNumberOfKeysInSegmentIndexPage;

    /*
     * SST index configuration
     */

    private final int maxNumberOfKeysInSCache;
    private final int maxNumberOfKeysInSegment;
    private final int maxNumberOfSegmentsInCache;

    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;

    SsstIndexConf(final long maxNumberOfKeysInSegmentCache,
            final long maxNumberOfKeysInSegmentCacheDuringFlushing,
            final int maxNumberOfKeysInSegmentIndexPage,
            final int maxNumberOfKeysInCache,
            final int maxNumberOfKeysInSegment,
            final int maxNumberOfSegmentsInCache,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        this.maxNumberOfKeysInSCache = maxNumberOfKeysInCache;
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
        return maxNumberOfKeysInSCache;
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

    long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return maxNumberOfKeysInSegmentCacheDuringFlushing;
    }

}
