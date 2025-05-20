package com.coroptis.index.sst;

import java.util.Objects;

public class IndexConfiguration {

    /*
     * Segments configuration
     */

    private final long maxNumberOfKeysInSegmentCache;
    private final long maxNumberOfKeysInSegmentCacheDuringFlushing;
    private final int maxNumberOfKeysInSegmentIndexPage;

    /*
     * SST index configuration
     */
    private final String indexName;
    private final int maxNumberOfKeysInSCache;
    private final int maxNumberOfKeysInSegment;
    private final int maxNumberOfSegmentsInCache;

    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;

    private final int diskIoBufferSize;
    private final boolean threadSafe;
    private final boolean logEnabled;

    static <M, N> IndexConfigurationBuilder<M, N> builder() {
        return new IndexConfigurationBuilder<>();
    }

    IndexConfiguration(final long maxNumberOfKeysInSegmentCache, //
            final long maxNumberOfKeysInSegmentCacheDuringFlushing, //
            final int maxNumberOfKeysInSegmentIndexPage, //
            final int maxNumberOfKeysInCache, //
            final int maxNumberOfKeysInSegment, //
            final int maxNumberOfSegmentsInCache, //
            final String indexName, //
            final Integer bloomFilterNumberOfHashFunctions, //
            final Integer bloomFilterIndexSizeInBytes, //
            final Double bloomFilterProbabilityOfFalsePositive, //
            final int diskIoBufferSize, final boolean threadSafe,
            final boolean logEnabled) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        this.indexName = Objects.requireNonNull(indexName,
                "Index name can't be null");
        this.maxNumberOfKeysInSCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
        this.threadSafe = threadSafe;
        this.logEnabled = logEnabled;

        if (diskIoBufferSize % 1024 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Parameter 'diskIoBufferSize' vith value '%s'"
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

    String getIndexName() {
        return indexName;
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

    boolean isThreadSafe() {
        return threadSafe;
    }

    boolean isLogEnabled() {
        return logEnabled;
    }
}
