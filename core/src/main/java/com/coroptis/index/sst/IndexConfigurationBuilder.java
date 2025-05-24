package com.coroptis.index.sst;

import com.coroptis.index.Vldtn;
import com.coroptis.index.datatype.TypeDescriptor;

public class IndexConfigurationBuilder<K, V> {

    private Long maxNumberOfKeysInSegmentCache;
    private Long maxNumberOfKeysInSegmentCacheDuringFlushing;
    private Integer maxNumberOfKeysInSegmentIndexPage;
    private Integer maxNumberOfKeysInCache;
    private Integer maxNumberOfKeysInSegment;
    private Integer maxNumberOfSegmentsInCache;

    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive;

    private int diskIoBufferSizeInBytes;

    private String indexName;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private String keyTypeDescriptor;
    private String valueTypeDescriptor;
    private Boolean logEnabled;
    private Boolean isThreadSafe;

    IndexConfigurationBuilder() {

    }

    public IndexConfigurationBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn
                .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor")
                .getClass().getName();
        return this;
    }

    public IndexConfigurationBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn
                .requireNonNull(valueTypeDescriptor, "valueTypeDescriptor")
                .getClass().getName();
        return this;
    }

    public IndexConfigurationBuilder<K, V> withKeyTypeDescriptor(
            final String keyTypeDescriptor) {
        this.keyTypeDescriptor = keyTypeDescriptor;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withValueTypeDescriptor(
            final String valueTypeDescriptor) {
        this.valueTypeDescriptor = valueTypeDescriptor;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withKeyClass(
            final Class<K> keyClass) {
        this.keyClass = keyClass;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withValueClass(
            final Class<V> valueClass) {
        this.valueClass = valueClass;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withName(final String indexName) {
        this.indexName = indexName;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final Long maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentIndexPage(
            final Integer maxNumberOfKeysInSegmentIndexPage) {
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInCache(
            final Integer maxNumberOfKeysInCache) {
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegment(
            final Integer maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfSegmentsInCache(
            final Integer maxNumberOfSegmentsInCache) {
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentCacheDuringFlushing(
            final Long maxNumberOfKeysInSegmentCacheDuringFlushing) {
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withBloomFilterNumberOfHashFunctions(
            final Integer bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withBloomFilterProbabilityOfFalsePositive(
            final Double probabilityOfFalsePositive) {
        this.bloomFilterProbabilityOfFalsePositive = probabilityOfFalsePositive;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withThreadSafe(
            final Boolean isThreadSafe) {
        this.isThreadSafe = isThreadSafe;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withBloomFilterIndexSizeInBytes(
            final Integer bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withDiskIoBufferSizeInBytes(
            final Integer diskIoBufferSizeInBytes) {
        this.diskIoBufferSizeInBytes = diskIoBufferSizeInBytes;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withLogEnabled(
            final Boolean useFullLog) {
        this.logEnabled = useFullLog;
        return this;
    }

    public IndexConfiguration<K, V> build() {
        final IndexConfiguration<K, V> indexConf = new IndexConfiguration<K, V>(
                keyClass, valueClass, keyTypeDescriptor, valueTypeDescriptor,
                maxNumberOfKeysInSegmentCache,
                maxNumberOfKeysInSegmentCacheDuringFlushing,
                maxNumberOfKeysInSegmentIndexPage, maxNumberOfKeysInCache,
                maxNumberOfKeysInSegment, maxNumberOfSegmentsInCache, indexName,
                bloomFilterNumberOfHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, diskIoBufferSizeInBytes,
                isThreadSafe, logEnabled);
        return indexConf;
    }

}
