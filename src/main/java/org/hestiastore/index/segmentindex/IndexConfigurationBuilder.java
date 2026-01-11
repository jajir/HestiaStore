package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.datatype.TypeDescriptor;

public class IndexConfigurationBuilder<K, V> {

    private Integer maxNumberOfKeysInSegmentCache;
    private Integer maxNumberOfKeysInSegmentWriteCache;
    private Integer maxNumberOfKeysInSegmentWriteCacheDuringFlush;
    private Integer maxNumberOfKeysInSegmentChunk;
    private Integer maxNumberOfKeysInCache;
    private Integer maxNumberOfKeysInSegment;
    private Integer maxNumberOfSegmentsInCache;

    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive;

    private Integer diskIoBufferSizeInBytes;
    private Integer numberOfThreads;
    private Integer numberOfIoThreads;
    private Integer numberOfSegmentIndexMaintenanceThreads;
    private Integer indexBusyBackoffMillis;
    private Integer indexBusyTimeoutMillis;
    private Boolean segmentMaintenanceAutoEnabled;

    private String indexName;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private String keyTypeDescriptor;
    private String valueTypeDescriptor;
    private Boolean contextLoggingEnabled;
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
            final Integer maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentWriteCache(
            final Integer maxNumberOfKeysInSegmentWriteCache) {
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentChunk(
            final Integer maxNumberOfKeysInSegmentChunk) {
        this.maxNumberOfKeysInSegmentChunk = maxNumberOfKeysInSegmentChunk;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(
            final Integer maxNumberOfKeysInSegmentWriteCacheDuringFlush) {
        this.maxNumberOfKeysInSegmentWriteCacheDuringFlush = maxNumberOfKeysInSegmentWriteCacheDuringFlush;
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

    public IndexConfigurationBuilder<K, V> withContextLoggingEnabled(
            final Boolean enabled) {
        this.contextLoggingEnabled = enabled;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withNumberOfCpuThreads(
            final Integer numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withNumberOfIoThreads(
            final Integer numberOfIoThreads) {
        this.numberOfIoThreads = numberOfIoThreads;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withNumberOfSegmentIndexMaintenanceThreads(
            final Integer numberOfSegmentIndexMaintenanceThreads) {
        this.numberOfSegmentIndexMaintenanceThreads = numberOfSegmentIndexMaintenanceThreads;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withIndexBusyBackoffMillis(
            final Integer indexBusyBackoffMillis) {
        this.indexBusyBackoffMillis = indexBusyBackoffMillis;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withIndexBusyTimeoutMillis(
            final Integer indexBusyTimeoutMillis) {
        this.indexBusyTimeoutMillis = indexBusyTimeoutMillis;
        return this;
    }

    public IndexConfigurationBuilder<K, V> withSegmentMaintenanceAutoEnabled(
            final Boolean segmentMaintenanceAutoEnabled) {
        this.segmentMaintenanceAutoEnabled = segmentMaintenanceAutoEnabled;
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
        final Integer effectiveNumberOfThreads = numberOfThreads == null
                ? IndexConfigurationContract.NUMBER_OF_THREADS
                : numberOfThreads;
        final Integer effectiveNumberOfIoThreads = numberOfIoThreads == null
                ? IndexConfigurationContract.NUMBER_OF_IO_THREADS
                : numberOfIoThreads;
        final Integer effectiveSegmentIndexMaintenanceThreads = numberOfSegmentIndexMaintenanceThreads == null
                ? IndexConfigurationContract.DEFAULT_SEGMENT_INDEX_MAINTENANCE_THREADS
                : numberOfSegmentIndexMaintenanceThreads;
        final Integer effectiveIndexBusyBackoffMillis = indexBusyBackoffMillis == null
                ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS
                : indexBusyBackoffMillis;
        final Integer effectiveIndexBusyTimeoutMillis = indexBusyTimeoutMillis == null
                ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS
                : indexBusyTimeoutMillis;
        final Boolean effectiveSegmentMaintenanceAutoEnabled = segmentMaintenanceAutoEnabled == null
                ? IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_AUTO_ENABLED
                : segmentMaintenanceAutoEnabled;
        final Integer effectiveWriteCacheDuringFlush;
        if (maxNumberOfKeysInSegmentWriteCacheDuringFlush == null
                && maxNumberOfKeysInSegmentWriteCache != null) {
            effectiveWriteCacheDuringFlush = Math.max(
                    (int) Math.ceil(maxNumberOfKeysInSegmentWriteCache * 1.4),
                    maxNumberOfKeysInSegmentWriteCache + 1);
        } else if (maxNumberOfKeysInSegmentWriteCacheDuringFlush == null) {
            effectiveWriteCacheDuringFlush = null;
        } else if (maxNumberOfKeysInSegmentWriteCache == null) {
            effectiveWriteCacheDuringFlush = Vldtn.requireGreaterThanZero(
                    maxNumberOfKeysInSegmentWriteCacheDuringFlush,
                    "maxNumberOfKeysInSegmentWriteCacheDuringFlush");
        } else {
            if (maxNumberOfKeysInSegmentWriteCacheDuringFlush <= maxNumberOfKeysInSegmentWriteCache) {
                throw new IllegalArgumentException(String.format(
                        "Property '%s' must be greater than '%s'",
                        "maxNumberOfKeysInSegmentWriteCacheDuringFlush",
                        "maxNumberOfKeysInSegmentWriteCache"));
            }
            effectiveWriteCacheDuringFlush = maxNumberOfKeysInSegmentWriteCacheDuringFlush;
        }
        return new IndexConfiguration<K, V>(keyClass, valueClass,
                keyTypeDescriptor, valueTypeDescriptor,
                maxNumberOfKeysInSegmentCache,
                maxNumberOfKeysInSegmentWriteCache,
                effectiveWriteCacheDuringFlush,
                maxNumberOfKeysInSegmentChunk, maxNumberOfKeysInCache,
                maxNumberOfKeysInSegment, maxNumberOfSegmentsInCache, indexName,
                bloomFilterNumberOfHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, diskIoBufferSizeInBytes,
                contextLoggingEnabled, effectiveNumberOfThreads,
                effectiveNumberOfIoThreads,
                effectiveSegmentIndexMaintenanceThreads,
                effectiveIndexBusyBackoffMillis,
                effectiveIndexBusyTimeoutMillis,
                effectiveSegmentMaintenanceAutoEnabled,
                encodingChunkFilters, decodingChunkFilters);
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
