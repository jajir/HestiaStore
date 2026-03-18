package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Fluent builder for {@link IndexConfiguration} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexConfigurationBuilder<K, V> {

    private static final String PROPERTY_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER =
            "maxNumberOfKeysInPartitionBuffer";

    private Integer maxNumberOfKeysInSegmentCache;
    private Integer maxNumberOfKeysInActivePartition;
    private Integer maxNumberOfKeysInPartitionBuffer;
    private Integer maxNumberOfImmutableRunsPerPartition;
    private Integer maxNumberOfKeysInIndexBuffer;
    private Integer maxNumberOfKeysInSegmentChunk;
    private Integer maxNumberOfDeltaCacheFiles;
    private Integer maxNumberOfKeysInSegment;
    private Integer maxNumberOfKeysInPartitionBeforeSplit;
    private Integer maxNumberOfSegmentsInCache;

    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive;

    private Integer diskIoBufferSizeInBytes;
    private Integer indexWorkerThreadCount;
    private Integer numberOfStableSegmentMaintenanceThreads;
    private Integer numberOfIndexMaintenanceThreads;
    private Integer numberOfRegistryLifecycleThreads;
    private Integer indexBusyBackoffMillis;
    private Integer indexBusyTimeoutMillis;
    private Boolean backgroundMaintenanceAutoEnabled;

    private String indexName;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private String keyTypeDescriptor;
    private String valueTypeDescriptor;
    private Boolean contextLoggingEnabled;
    private Wal wal = Wal.EMPTY;
    private final List<ChunkFilter> encodingChunkFilters = new ArrayList<>();
    private final List<ChunkFilter> decodingChunkFilters = new ArrayList<>();

    IndexConfigurationBuilder() {

    }

    /**
     * Sets the key type descriptor instance used for serialization.
     *
     * @param keyTypeDescriptor type descriptor for keys
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn
                .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor")
                .getClass().getName();
        return this;
    }

    /**
     * Sets the value type descriptor instance used for serialization.
     *
     * @param valueTypeDescriptor type descriptor for values
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn
                .requireNonNull(valueTypeDescriptor, "valueTypeDescriptor")
                .getClass().getName();
        return this;
    }

    /**
     * Sets the fully qualified class name of the key type descriptor.
     *
     * @param keyTypeDescriptor class name for the key type descriptor
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withKeyTypeDescriptor(
            final String keyTypeDescriptor) {
        this.keyTypeDescriptor = keyTypeDescriptor;
        return this;
    }

    /**
     * Sets the fully qualified class name of the value type descriptor.
     *
     * @param valueTypeDescriptor class name for the value type descriptor
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withValueTypeDescriptor(
            final String valueTypeDescriptor) {
        this.valueTypeDescriptor = valueTypeDescriptor;
        return this;
    }

    /**
     * Sets the key class for the index.
     *
     * @param keyClass key class
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withKeyClass(
            final Class<K> keyClass) {
        this.keyClass = keyClass;
        return this;
    }

    /**
     * Sets the value class for the index.
     *
     * @param valueClass value class
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withValueClass(
            final Class<V> valueClass) {
        this.valueClass = valueClass;
        return this;
    }

    /**
     * Sets the logical index name.
     *
     * @param indexName name of the index
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withName(final String indexName) {
        this.indexName = indexName;
        return this;
    }

    /**
     * Sets the max number of keys held in the in-memory segment cache.
     *
     * @param maxNumberOfKeysInSegmentCache max keys in segment cache
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final Integer maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        return this;
    }

    /**
     * Sets the maximum number of keys accepted into the active partition
     * before it is rotated to an immutable run.
     *
     * @param maxNumberOfKeysInActivePartition max active partition keys
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInActivePartition(
            final Integer maxNumberOfKeysInActivePartition) {
        this.maxNumberOfKeysInActivePartition = maxNumberOfKeysInActivePartition;
        return this;
    }

    /**
     * Sets the max number of keys per on-disk segment chunk.
     *
     * @param maxNumberOfKeysInSegmentChunk max keys per chunk
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegmentChunk(
            final Integer maxNumberOfKeysInSegmentChunk) {
        this.maxNumberOfKeysInSegmentChunk = maxNumberOfKeysInSegmentChunk;
        return this;
    }

    /**
     * Sets the max number of delta cache files allowed per segment.
     *
     * @param maxNumberOfDeltaCacheFiles max delta cache file count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfDeltaCacheFiles(
            final Integer maxNumberOfDeltaCacheFiles) {
        this.maxNumberOfDeltaCacheFiles = maxNumberOfDeltaCacheFiles;
        return this;
    }

    /**
     * Sets the maximum immutable run queue depth per partition.
     *
     * @param maxNumberOfImmutableRunsPerPartition immutable run count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfImmutableRunsPerPartition(
            final Integer maxNumberOfImmutableRunsPerPartition) {
        this.maxNumberOfImmutableRunsPerPartition = maxNumberOfImmutableRunsPerPartition;
        return this;
    }

    /**
     * Sets the maximum number of keys buffered inside one partition.
     *
     * @param maxNumberOfKeysInPartitionBuffer per-partition buffered key count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInPartitionBuffer(
            final Integer maxNumberOfKeysInPartitionBuffer) {
        this.maxNumberOfKeysInPartitionBuffer = maxNumberOfKeysInPartitionBuffer;
        return this;
    }

    /**
     * Sets the max number of keys allowed within a segment.
     *
     * @param maxNumberOfKeysInSegment max keys per segment
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInSegment(
            final Integer maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        return this;
    }

    /**
     * Sets the maximum number of keys buffered across the whole index overlay.
     *
     * @param maxNumberOfKeysInIndexBuffer global buffered key count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInIndexBuffer(
            final Integer maxNumberOfKeysInIndexBuffer) {
        this.maxNumberOfKeysInIndexBuffer = maxNumberOfKeysInIndexBuffer;
        return this;
    }

    /**
     * Sets the threshold at which a partition becomes eligible for split or
     * drain re-routing.
     *
     * @param maxNumberOfKeysInPartitionBeforeSplit max keys before split
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfKeysInPartitionBeforeSplit(
            final Integer maxNumberOfKeysInPartitionBeforeSplit) {
        this.maxNumberOfKeysInPartitionBeforeSplit = maxNumberOfKeysInPartitionBeforeSplit;
        return this;
    }

    /**
     * Sets the max number of segments retained in the in-memory cache.
     *
     * @param maxNumberOfSegmentsInCache max segments in cache
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withMaxNumberOfSegmentsInCache(
            final Integer maxNumberOfSegmentsInCache) {
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        return this;
    }

    /**
     * Sets the number of hash functions used by the Bloom filter.
     *
     * @param bloomFilterNumberOfHashFunctions Bloom filter hash function count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withBloomFilterNumberOfHashFunctions(
            final Integer bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        return this;
    }

    /**
     * Sets the target Bloom filter false-positive probability.
     *
     * @param probabilityOfFalsePositive false-positive probability
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withBloomFilterProbabilityOfFalsePositive(
            final Double probabilityOfFalsePositive) {
        this.bloomFilterProbabilityOfFalsePositive = probabilityOfFalsePositive;
        return this;
    }

    /**
     * Sets the Bloom filter index size in bytes.
     *
     * @param bloomFilterIndexSizeInBytes Bloom filter size in bytes
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withBloomFilterIndexSizeInBytes(
            final Integer bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        return this;
    }

    /**
     * Sets the disk I/O buffer size in bytes.
     *
     * @param diskIoBufferSizeInBytes buffer size in bytes
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withDiskIoBufferSizeInBytes(
            final Integer diskIoBufferSizeInBytes) {
        this.diskIoBufferSizeInBytes = diskIoBufferSizeInBytes;
        return this;
    }

    /**
     * Sets whether MDC-based context logging is enabled.
     *
     * @param enabled true to enable context logging
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withContextLoggingEnabled(
            final Boolean enabled) {
        this.contextLoggingEnabled = enabled;
        return this;
    }

    /**
     * Sets WAL configuration. {@code null} is normalized to
     * {@link Wal#EMPTY}.
     *
     * @param wal WAL configuration
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withWal(final Wal wal) {
        this.wal = Wal.orEmpty(wal);
        return this;
    }

    /**
     * Sets the number of index worker threads used for index operations.
     *
     * @param indexWorkerThreadCount index worker thread count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withIndexWorkerThreadCount(
            final Integer indexWorkerThreadCount) {
        this.indexWorkerThreadCount = indexWorkerThreadCount;
        return this;
    }

    /**
     * Sets the number of stable-segment maintenance threads.
     *
     * @param numberOfStableSegmentMaintenanceThreads stable-segment
     *                                               maintenance threads
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withNumberOfStableSegmentMaintenanceThreads(
            final Integer numberOfStableSegmentMaintenanceThreads) {
        this.numberOfStableSegmentMaintenanceThreads = numberOfStableSegmentMaintenanceThreads;
        return this;
    }

    /**
     * Sets the number of split maintenance threads.
     *
     * @param numberOfIndexMaintenanceThreads split maintenance thread count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withNumberOfIndexMaintenanceThreads(
            final Integer numberOfIndexMaintenanceThreads) {
        this.numberOfIndexMaintenanceThreads = numberOfIndexMaintenanceThreads;
        return this;
    }

    /**
     * Sets the number of registry lifecycle threads used for
     * segment load/unload operations.
     *
     * @param numberOfRegistryLifecycleThreads registry lifecycle thread count
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withNumberOfRegistryLifecycleThreads(
            final Integer numberOfRegistryLifecycleThreads) {
        this.numberOfRegistryLifecycleThreads = numberOfRegistryLifecycleThreads;
        return this;
    }

    /**
     * Sets the busy backoff delay in milliseconds.
     *
     * @param indexBusyBackoffMillis backoff delay in milliseconds
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withIndexBusyBackoffMillis(
            final Integer indexBusyBackoffMillis) {
        this.indexBusyBackoffMillis = indexBusyBackoffMillis;
        return this;
    }

    /**
     * Sets the busy retry timeout in milliseconds.
     *
     * @param indexBusyTimeoutMillis busy timeout in milliseconds
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withIndexBusyTimeoutMillis(
            final Integer indexBusyTimeoutMillis) {
        this.indexBusyTimeoutMillis = indexBusyTimeoutMillis;
        return this;
    }

    /**
     * Sets whether auto maintenance is enabled after writes.
     *
     * @param backgroundMaintenanceAutoEnabled true to enable auto maintenance
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withBackgroundMaintenanceAutoEnabled(
            final Boolean backgroundMaintenanceAutoEnabled) {
        this.backgroundMaintenanceAutoEnabled = backgroundMaintenanceAutoEnabled;
        return this;
    }

    /**
     * Adds a chunk filter to the encoding pipeline.
     *
     * @param filter filter to add
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> addEncodingFilter(
            final ChunkFilter filter) {
        encodingChunkFilters.add(Vldtn.requireNonNull(filter, "filter"));
        return this;
    }

    /**
     * Adds a chunk filter class to the encoding pipeline.
     *
     * @param filterClass filter class to instantiate
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> addEncodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        return addEncodingFilter(instantiateFilter(filterClass));
    }

    /**
     * Replaces the encoding filter pipeline with the supplied filter classes.
     *
     * @param filterClasses filter classes to instantiate
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withEncodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> filterClasses) {
        Vldtn.requireNonNull(filterClasses, "filterClasses");
        encodingChunkFilters.clear();
        for (final Class<? extends ChunkFilter> filterClass : filterClasses) {
            addEncodingFilter(filterClass);
        }
        return this;
    }

    /**
     * Replaces the encoding filter pipeline with the supplied filters.
     *
     * @param filters filters to use
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withEncodingFilters(
            final Collection<ChunkFilter> filters) {
        Vldtn.requireNonNull(filters, "filters");
        encodingChunkFilters.clear();
        for (final ChunkFilter filter : filters) {
            addEncodingFilter(filter);
        }
        return this;
    }

    /**
     * Adds a chunk filter to the decoding pipeline.
     *
     * @param filter filter to add
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> addDecodingFilter(
            final ChunkFilter filter) {
        decodingChunkFilters.add(Vldtn.requireNonNull(filter, "filter"));
        return this;
    }

    /**
     * Adds a chunk filter class to the decoding pipeline.
     *
     * @param filterClass filter class to instantiate
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> addDecodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        return addDecodingFilter(instantiateFilter(filterClass));
    }

    /**
     * Replaces the decoding filter pipeline with the supplied filter classes.
     *
     * @param filterClasses filter classes to instantiate
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withDecodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> filterClasses) {
        Vldtn.requireNonNull(filterClasses, "filterClasses");
        decodingChunkFilters.clear();
        for (final Class<? extends ChunkFilter> filterClass : filterClasses) {
            addDecodingFilter(filterClass);
        }
        return this;
    }

    /**
     * Replaces the decoding filter pipeline with the supplied filters.
     *
     * @param filters filters to use
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> withDecodingFilters(
            final Collection<ChunkFilter> filters) {
        Vldtn.requireNonNull(filters, "filters");
        decodingChunkFilters.clear();
        for (final ChunkFilter filter : filters) {
            addDecodingFilter(filter);
        }
        return this;
    }

    /**
     * Builds an immutable {@link IndexConfiguration} from the collected
     * settings.
     *
     * @return built configuration
     */
    public IndexConfiguration<K, V> build() {
        final Integer effectiveIndexWorkerThreadCount = indexWorkerThreadCount == null
                ? IndexConfigurationContract.INDEX_WORKER_THREAD_COUNT
                : indexWorkerThreadCount;
        final Integer effectiveStableSegmentMaintenanceThreads = numberOfStableSegmentMaintenanceThreads == null
                ? IndexConfigurationContract.DEFAULT_STABLE_SEGMENT_MAINTENANCE_THREADS
                : numberOfStableSegmentMaintenanceThreads;
        final Integer effectiveIndexMaintenanceThreads = numberOfIndexMaintenanceThreads == null
                ? IndexConfigurationContract.DEFAULT_INDEX_MAINTENANCE_THREADS
                : numberOfIndexMaintenanceThreads;
        final Integer effectiveRegistryLifecycleThreads = numberOfRegistryLifecycleThreads == null
                ? IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS
                : numberOfRegistryLifecycleThreads;
        final Integer effectiveIndexBusyBackoffMillis = indexBusyBackoffMillis == null
                ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS
                : indexBusyBackoffMillis;
        final Integer effectiveIndexBusyTimeoutMillis = indexBusyTimeoutMillis == null
                ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS
                : indexBusyTimeoutMillis;
        final Boolean effectiveBackgroundMaintenanceAutoEnabled = backgroundMaintenanceAutoEnabled == null
                ? IndexConfigurationContract.DEFAULT_BACKGROUND_MAINTENANCE_AUTO_ENABLED
                : backgroundMaintenanceAutoEnabled;
        final Integer effectiveMaxNumberOfDeltaCacheFiles = maxNumberOfDeltaCacheFiles == null
                ? IndexConfigurationContract.MAX_NUMBER_OF_DELTA_CACHE_FILES
                : maxNumberOfDeltaCacheFiles;
        final Integer effectiveMaxNumberOfImmutableRunsPerPartition = maxNumberOfImmutableRunsPerPartition == null
                ? IndexConfigurationContract.DEFAULT_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION
                : maxNumberOfImmutableRunsPerPartition;
        final Integer effectiveMaxNumberOfKeysInSegment = resolveEffectiveMaxNumberOfKeysInSegment();
        final Integer effectiveMaxNumberOfKeysInPartitionBeforeSplit = resolveEffectiveMaxNumberOfKeysInPartitionBeforeSplit(
                effectiveMaxNumberOfKeysInSegment);
        final Integer effectivePartitionBuffer = resolveEffectivePartitionBuffer();
        final Integer effectiveIndexBuffer = resolveEffectiveIndexBuffer(
                effectivePartitionBuffer);
        return new IndexConfiguration<K, V>(keyClass, valueClass,
                keyTypeDescriptor, valueTypeDescriptor,
                maxNumberOfKeysInSegmentCache,
                maxNumberOfKeysInActivePartition,
                effectivePartitionBuffer,
                effectiveMaxNumberOfImmutableRunsPerPartition,
                effectiveIndexBuffer,
                maxNumberOfKeysInSegmentChunk, effectiveMaxNumberOfDeltaCacheFiles,
                effectiveMaxNumberOfKeysInSegment,
                effectiveMaxNumberOfKeysInPartitionBeforeSplit,
                maxNumberOfSegmentsInCache, indexName,
                bloomFilterNumberOfHashFunctions, bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive, diskIoBufferSizeInBytes,
                contextLoggingEnabled, effectiveIndexWorkerThreadCount,
                effectiveStableSegmentMaintenanceThreads,
                effectiveIndexMaintenanceThreads,
                effectiveRegistryLifecycleThreads,
                effectiveIndexBusyBackoffMillis,
                effectiveIndexBusyTimeoutMillis,
                effectiveBackgroundMaintenanceAutoEnabled,
                Wal.orEmpty(wal),
                encodingChunkFilters, decodingChunkFilters);
    }

    private Integer resolveEffectiveMaxNumberOfKeysInSegment() {
        if (maxNumberOfKeysInSegment != null) {
            return maxNumberOfKeysInSegment;
        }
        return maxNumberOfKeysInPartitionBeforeSplit;
    }

    private Integer resolveEffectiveMaxNumberOfKeysInPartitionBeforeSplit(
            final Integer effectiveMaxNumberOfKeysInSegment) {
        if (maxNumberOfKeysInPartitionBeforeSplit != null) {
            return maxNumberOfKeysInPartitionBeforeSplit;
        }
        return effectiveMaxNumberOfKeysInSegment;
    }

    private Integer resolveEffectivePartitionBuffer() {
        if (maxNumberOfKeysInPartitionBuffer == null
                && maxNumberOfKeysInActivePartition != null) {
            return Math.max(
                    (int) Math.ceil(maxNumberOfKeysInActivePartition * 1.4),
                    maxNumberOfKeysInActivePartition + 1);
        }
        if (maxNumberOfKeysInPartitionBuffer == null) {
            return null;
        }
        if (maxNumberOfKeysInActivePartition == null) {
            return Vldtn.requireGreaterThanZero(
                    maxNumberOfKeysInPartitionBuffer,
                    PROPERTY_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER);
        }
        if (maxNumberOfKeysInPartitionBuffer <= maxNumberOfKeysInActivePartition) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be greater than '%s'",
                    PROPERTY_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                    "maxNumberOfKeysInActivePartition"));
        }
        return maxNumberOfKeysInPartitionBuffer;
    }

    private Integer resolveEffectiveIndexBuffer(
            final Integer effectivePartitionBuffer) {
        if (maxNumberOfKeysInIndexBuffer != null) {
            if (effectivePartitionBuffer != null
                    && maxNumberOfKeysInIndexBuffer.intValue() < effectivePartitionBuffer
                            .intValue()) {
                throw new IllegalArgumentException(String.format(
                        "Property '%s' must be greater than or equal to '%s'",
                        "maxNumberOfKeysInIndexBuffer",
                        PROPERTY_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER));
            }
            return maxNumberOfKeysInIndexBuffer;
        }
        if (effectivePartitionBuffer == null) {
            return null;
        }
        final int segmentCount = maxNumberOfSegmentsInCache == null
                ? IndexConfigurationContract.MAX_NUMBER_OF_SEGMENTS_IN_CACHE
                : maxNumberOfSegmentsInCache.intValue();
        return Integer.valueOf(Math.max(effectivePartitionBuffer.intValue(),
                effectivePartitionBuffer.intValue() * Math.max(1, segmentCount)));
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
