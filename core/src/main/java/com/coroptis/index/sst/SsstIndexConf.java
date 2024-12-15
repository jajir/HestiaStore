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

    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;

    private final int diskIoBufferSize;

    SsstIndexConf(final long maxNumberOfKeysInSegmentCache,
            final long maxNumberOfKeysInSegmentCacheDuringFlushing,
            final int maxNumberOfKeysInSegmentIndexPage,
            final int maxNumberOfKeysInCache,
            final int maxNumberOfKeysInSegment,
            final int maxNumberOfSegmentsInCache,
            final Integer bloomFilterNumberOfHashFunctions,
            final Integer bloomFilterIndexSizeInBytes,
            final Double bloomFilterProbabilityOfFalsePositive,
            final int diskIoBufferSize) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        this.maxNumberOfKeysInSCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;

        if (diskIoBufferSize % 1024 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'indexBufferSizeInBytes' vith value '%s'"
                            + " can't be divided by 1024 without reminder",
                    diskIoBufferSize));
        }
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

    Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    int getMaxNumberOfSegmentsInCache() {
        return maxNumberOfSegmentsInCache;
    }

    long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return maxNumberOfKeysInSegmentCacheDuringFlushing;
    }

    int getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

}
