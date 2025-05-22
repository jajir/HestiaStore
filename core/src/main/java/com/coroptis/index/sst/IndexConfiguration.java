package com.coroptis.index.sst;

import com.coroptis.index.datatype.TypeDescriptor;

public class IndexConfiguration<K, V> {

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
    private final Boolean threadSafe;
    private final Boolean logEnabled;

    /**
     * Creates a new instance of IndexConfigurationBuilder.
     *
     * @param <M> the type of the key
     * @param <N> the type of the value
     * @return a new instance of IndexConfigurationBuilder
     */
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
            final int diskIoBufferSize, final Boolean threadSafe,
            final Boolean logEnabled) {
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

    int getMaxNumberOfKeysInCache() {
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

    Boolean isThreadSafe() {
        return threadSafe;
    }

    Boolean isLogEnabled() {
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
