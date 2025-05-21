package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;

public class IndexConfiguration<K, V> {

    private final static long DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 200_000;
    private final static long DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = -1;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE = 5_000;

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT = 10_000_000;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE = 1_000_000;
    private final static int DEFAULT_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = 10;

    private final static boolean DEFAULT_INDEX_SYNCHRONIZED = false;

    private final static int DEFAULT_DISK_IO_BUFFER_SIZE_IN_BYTES = 1024 * 4;

    /**
     * general Data configuration.
     */
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

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
    // TODO with method shoul be withThreadSafe and withLogging
    // TODO remove with custom conf, it's useless

    public static <M, N> IndexConfigurationBuilder<M, N> builder() {
        return new IndexConfigurationBuilder<>();
    }

    IndexConfiguration(final Class<K> keyClass, //
            final Class<V> valueClass, //
            final TypeDescriptor<K> keyTypeDescriptor, //
            final TypeDescriptor<V> valueTypeDescriptor, //
            final long maxNumberOfKeysInSegmentCache, //
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
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        this.indexName = indexName;
        this.maxNumberOfKeysInSCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
        this.threadSafe = threadSafe;
        this.logEnabled = logEnabled;
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

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public TypeDescriptor<K> getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    public TypeDescriptor<V> getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }
}
