package com.hestiastore.index.sst;

public class IndexConfiguration<K, V> {

    /**
     * general Data configuration.
     */
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String keyTypeDescriptor;
    private final String valueTypeDescriptor;

    /*
     * Segments configuration
     */
    private final Long maxNumberOfKeysInSegmentCache;
    private final Long maxNumberOfKeysInSegmentCacheDuringFlushing;
    private final Integer maxNumberOfKeysInSegmentIndexPage;

    /*
     * SST index configuration
     */
    private final String indexName;
    private final Integer maxNumberOfKeysInSCache;
    private final Integer maxNumberOfKeysInSegment;
    private final Integer maxNumberOfSegmentsInCache;

    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;

    private final Integer diskIoBufferSize;
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
            final String keyTypeDescriptor, //
            final String valueTypeDescriptor, //
            final Long maxNumberOfKeysInSegmentCache, //
            final Long maxNumberOfKeysInSegmentCacheDuringFlushing, //
            final Integer maxNumberOfKeysInSegmentIndexPage, //
            final Integer maxNumberOfKeysInCache, //
            final Integer maxNumberOfKeysInSegment, //
            final Integer maxNumberOfSegmentsInCache, //
            final String indexName, //
            final Integer bloomFilterNumberOfHashFunctions, //
            final Integer bloomFilterIndexSizeInBytes, //
            final Double bloomFilterProbabilityOfFalsePositive, //
            final Integer diskIoBufferSize, final Boolean threadSafe,
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

    public Long getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    public Integer getMaxNumberOfKeysInSegmentIndexPage() {
        return maxNumberOfKeysInSegmentIndexPage;
    }

    public String getIndexName() {
        return indexName;
    }

    public Integer getMaxNumberOfKeysInCache() {
        return maxNumberOfKeysInSCache;
    }

    public Integer getMaxNumberOfKeysInSegment() {
        return maxNumberOfKeysInSegment;
    }

    public Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    public Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    public Integer getMaxNumberOfSegmentsInCache() {
        return maxNumberOfSegmentsInCache;
    }

    public Long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return maxNumberOfKeysInSegmentCacheDuringFlushing;
    }

    public Integer getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    public Boolean isThreadSafe() {
        return threadSafe;
    }

    public Boolean isLogEnabled() {
        return logEnabled;
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public String getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    public String getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }
}
