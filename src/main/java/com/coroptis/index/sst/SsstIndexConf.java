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
    private final int indexBufferSizeInBytes;

    SsstIndexConf(final long maxNumberOfKeysInSegmentCache,
            final long maxNumberOfKeysInSegmentCacheDuringFlushing,
            final int maxNumberOfKeysInSegmentIndexPage,
            final int maxNumberOfKeysInCache,
            final int maxNumberOfKeysInSegment,
            final int maxNumberOfSegmentsInCache,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final int indexBufferSizeInBytes) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        this.maxNumberOfKeysInSCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.indexBufferSizeInBytes = indexBufferSizeInBytes;
    }

    long getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    int getMaxNumberOfKeysInSegmentIndexPage() {
        return maxNumberOfKeysInSegmentIndexPage;
    }

    long getMaxNumberOfKeysInCache() {
        return maxNumberOfKeysInSCache;
    }

    int getMaxNumberOfKeysInSegment() {
        return maxNumberOfKeysInSegment;
    }

    int getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    int getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    int getMaxNumberOfSegmentsInCache() {
        return maxNumberOfSegmentsInCache;
    }

    long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return maxNumberOfKeysInSegmentCacheDuringFlushing;
    }

    int getIndexBufferSizeInBytes() {
        return indexBufferSizeInBytes;
    }

}
