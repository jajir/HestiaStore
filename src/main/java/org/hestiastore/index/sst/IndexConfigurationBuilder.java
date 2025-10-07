package org.hestiastore.index.sst;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.datatype.TypeDescriptor;

public class IndexConfigurationBuilder<K, V> {

    private Long maxNumberOfKeysInSegmentCache;
    private Long maxNumberOfKeysInSegmentCacheDuringFlushing;
    private Integer maxNumberOfKeysInSegmentChunk;
    private Integer maxNumberOfKeysInCache;
    private Integer maxNumberOfKeysInSegment;
    private Integer maxNumberOfSegmentsInCache;

    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive;

    private Integer diskIoBufferSizeInBytes;

    private String indexName;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private String keyTypeDescriptor;
    private String valueTypeDescriptor;
    private Boolean logEnabled;
    private Boolean isThreadSafe;
    private final List<ChunkFilter> encodingChunkFilters = new ArrayList<>();
    private final List<ChunkFilter> decodingChunkFilters = new ArrayList<>();

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

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentChunk(
            final Integer maxNumberOfKeysInSegmentChunk) {
        this.maxNumberOfKeysInSegmentChunk = maxNumberOfKeysInSegmentChunk;
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

    public IndexConfigurationBuilder<K, V> addEncodingFilter(
            final ChunkFilter filter) {
        encodingChunkFilters.add(Vldtn.requireNonNull(filter, "filter"));
        return this;
    }

    public IndexConfigurationBuilder<K, V> addEncodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        return addEncodingFilter(instantiateFilter(filterClass));
    }

    public IndexConfigurationBuilder<K, V> withEncodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> filterClasses) {
        Vldtn.requireNonNull(filterClasses, "filterClasses");
        encodingChunkFilters.clear();
        for (final Class<? extends ChunkFilter> filterClass : filterClasses) {
            addEncodingFilter(filterClass);
        }
        return this;
    }

    public IndexConfigurationBuilder<K, V> withEncodingFilters(
            final Collection<ChunkFilter> filters) {
        Vldtn.requireNonNull(filters, "filters");
        encodingChunkFilters.clear();
        for (final ChunkFilter filter : filters) {
            addEncodingFilter(filter);
        }
        return this;
    }

    public IndexConfigurationBuilder<K, V> addDecodingFilter(
            final ChunkFilter filter) {
        decodingChunkFilters.add(Vldtn.requireNonNull(filter, "filter"));
        return this;
    }

    public IndexConfigurationBuilder<K, V> addDecodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        return addDecodingFilter(instantiateFilter(filterClass));
    }

    public IndexConfigurationBuilder<K, V> withDecodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> filterClasses) {
        Vldtn.requireNonNull(filterClasses, "filterClasses");
        decodingChunkFilters.clear();
        for (final Class<? extends ChunkFilter> filterClass : filterClasses) {
            addDecodingFilter(filterClass);
        }
        return this;
    }

    public IndexConfigurationBuilder<K, V> withDecodingFilters(
            final Collection<ChunkFilter> filters) {
        Vldtn.requireNonNull(filters, "filters");
        decodingChunkFilters.clear();
        for (final ChunkFilter filter : filters) {
            addDecodingFilter(filter);
        }
        return this;
    }

    public IndexConfiguration<K, V> build() {
        return new IndexConfiguration<K, V>(keyClass, valueClass,
                keyTypeDescriptor, valueTypeDescriptor,
                maxNumberOfKeysInSegmentCache,
                maxNumberOfKeysInSegmentCacheDuringFlushing,
                maxNumberOfKeysInSegmentChunk, maxNumberOfKeysInCache,
                maxNumberOfKeysInSegment, maxNumberOfSegmentsInCache, indexName,
                bloomFilterNumberOfHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, diskIoBufferSizeInBytes,
                isThreadSafe, logEnabled, encodingChunkFilters,
                decodingChunkFilters);
    }

    private ChunkFilter instantiateFilter(
            final Class<? extends ChunkFilter> filterClass) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(filterClass, "filterClass");
        try {
            return requiredClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException ex) {
            throw new IllegalArgumentException(
                    String.format("Unable to instantiate chunk filter '%s'",
                            requiredClass.getName()),
                    ex);
        }
    }

}
