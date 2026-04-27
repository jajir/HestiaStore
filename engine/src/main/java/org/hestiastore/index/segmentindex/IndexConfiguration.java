package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;

/**
 * Immutable configuration for the segment-index layer.
 * <p>
 * Encapsulates key/value types, index name, segment sizing and caching limits,
 * Bloom filter parameters, disk I/O buffer size, logging switches, and the
 * chunk encoding/decoding filter pipeline metadata. Instances are created via
 * the fluent {@link IndexConfigurationBuilder}.
 *
 * @param <K> key type
 * @param <V> value type
 * @see IndexConfigurationBuilder
 */
@SuppressWarnings("java:S107")
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
    private final Integer maxNumberOfKeysInSegmentCache;
    private final Integer maxNumberOfKeysInSegmentChunk;
    private final Integer maxNumberOfDeltaCacheFiles;
    private final IndexWritePathConfiguration writePathConfiguration;
    private final LegacyPartitionCompatibilityConfiguration legacyPartitionCompatibilityConfiguration;

    /*
     * Segment index configuration
     */
    private final String indexName;
    private final Integer maxNumberOfKeysInSegment;
    private final Integer maxNumberOfSegmentsInCache;
    private final Integer numberOfSegmentMaintenanceThreads;
    private final Integer numberOfIndexMaintenanceThreads;
    private final Integer numberOfRegistryLifecycleThreads;
    private final Integer indexBusyBackoffMillis;
    private final Integer indexBusyTimeoutMillis;
    private final Boolean backgroundMaintenanceAutoEnabled;

    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;

    private final Integer diskIoBufferSize;
    private final Boolean contextLoggingEnabled;
    private final Wal wal;

    private final List<ChunkFilterSpec> encodingChunkFilters;
    private final List<ChunkFilterSpec> decodingChunkFilters;

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
            final Integer maxNumberOfKeysInSegmentCache, //
            final Integer maxNumberOfKeysInActivePartition, //
            final Integer maxNumberOfKeysInPartitionBuffer, //
            final Integer maxNumberOfImmutableRunsPerPartition, //
            final Integer maxNumberOfKeysInIndexBuffer, //
            final Integer maxNumberOfKeysInSegmentChunk, //
            final Integer maxNumberOfDeltaCacheFiles, //
            final Integer maxNumberOfKeysInSegment, //
            final Integer maxNumberOfKeysInPartitionBeforeSplit, //
            final Integer maxNumberOfSegmentsInCache, //
            final String indexName, //
            final Integer bloomFilterNumberOfHashFunctions, //
            final Integer bloomFilterIndexSizeInBytes, //
            final Double bloomFilterProbabilityOfFalsePositive, //
            final Integer diskIoBufferSize, final Boolean contextLoggingEnabled,
            final Integer numberOfSegmentMaintenanceThreads,
            final Integer numberOfIndexMaintenanceThreads,
            final Integer numberOfRegistryLifecycleThreads,
            final Integer indexBusyBackoffMillis,
            final Integer indexBusyTimeoutMillis,
            final Boolean backgroundMaintenanceAutoEnabled,
            final Wal wal,
            final List<ChunkFilterSpec> encodingChunkFilters,
            final List<ChunkFilterSpec> decodingChunkFilters) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentChunk = maxNumberOfKeysInSegmentChunk;
        this.maxNumberOfDeltaCacheFiles = maxNumberOfDeltaCacheFiles;
        this.writePathConfiguration = new IndexWritePathConfiguration(
                maxNumberOfKeysInActivePartition,
                maxNumberOfKeysInPartitionBuffer, maxNumberOfKeysInIndexBuffer,
                maxNumberOfKeysInPartitionBeforeSplit);
        this.legacyPartitionCompatibilityConfiguration = new LegacyPartitionCompatibilityConfiguration(
                writePathConfiguration, maxNumberOfImmutableRunsPerPartition);
        this.indexName = indexName;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.numberOfSegmentMaintenanceThreads = numberOfSegmentMaintenanceThreads;
        this.numberOfIndexMaintenanceThreads = numberOfIndexMaintenanceThreads;
        this.numberOfRegistryLifecycleThreads = numberOfRegistryLifecycleThreads;
        this.indexBusyBackoffMillis = indexBusyBackoffMillis;
        this.indexBusyTimeoutMillis = indexBusyTimeoutMillis;
        this.backgroundMaintenanceAutoEnabled = backgroundMaintenanceAutoEnabled;
        this.wal = Wal.orEmpty(wal);
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
        this.contextLoggingEnabled = contextLoggingEnabled;
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
    }

    /**
     * Returns the maximum number of keys held in the in-memory segment cache.
     *
     * @return max keys in segment cache
     */
    public Integer getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    /**
     * Returns canonical write-path configuration for the direct-to-segment
     * runtime.
     *
     * @return immutable write-path configuration
     */
    public IndexWritePathConfiguration getWritePathConfiguration() {
        return writePathConfiguration;
    }

    /**
     * Returns compatibility settings retained for callers that still consume
     * legacy partition-overlay naming.
     *
     * @return immutable compatibility view
     */
    public LegacyPartitionCompatibilityConfiguration getLegacyPartitionCompatibilityConfiguration() {
        return legacyPartitionCompatibilityConfiguration;
    }

    /**
     * Returns the maximum number of keys accepted into one routed segment write
     * cache in the current direct-to-segment runtime.
     *
     * @return max keys in one segment write cache
     */
    public Integer getSegmentWriteCacheKeyLimit() {
        return writePathConfiguration.getSegmentWriteCacheKeyLimit();
    }

    /**
     * Returns the maximum number of keys buffered in one routed segment while
     * maintenance is running.
     *
     * @return maintenance-time buffered key limit
     */
    public Integer getSegmentWriteCacheKeyLimitDuringMaintenance() {
        return writePathConfiguration
                .getSegmentWriteCacheKeyLimitDuringMaintenance();
    }

    /**
     * Returns the maximum number of buffered keys allowed across the whole
     * index.
     *
     * @return index-wide buffered key limit
     */
    public Integer getIndexBufferedWriteKeyLimit() {
        return writePathConfiguration.getIndexBufferedWriteKeyLimit();
    }

    /**
     * Returns the split threshold for one routed segment.
     *
     * @return max keys before one routed segment is split
     */
    public Integer getSegmentSplitKeyThreshold() {
        return writePathConfiguration.getSegmentSplitKeyThreshold();
    }

    /**
     * Returns the maximum number of keys accepted into the routed segment write
     * cache.
     * <p>
     * Use {@link #getSegmentWriteCacheKeyLimit()} for the canonical name.
     *
     * @return max routed write-cache keys
     * @deprecated use {@link #getSegmentWriteCacheKeyLimit()}
     */
    @Deprecated
    public Integer getMaxNumberOfKeysInActivePartition() {
        return legacyPartitionCompatibilityConfiguration
                .getMaxNumberOfKeysInActivePartition();
    }

    /**
     * Returns a legacy compatibility limit retained from the removed partition
     * runtime.
     *
     * @return legacy compatibility limit
     * @deprecated use canonical write-path settings via
     *             {@link #getWritePathConfiguration()}
     */
    @Deprecated
    public Integer getMaxNumberOfImmutableRunsPerPartition() {
        return legacyPartitionCompatibilityConfiguration
                .getMaxNumberOfImmutableRunsPerPartition();
    }

    /**
     * Returns the maximum number of buffered keys allowed inside one routed
     * segment before local backpressure is applied.
     * <p>
     * Use {@link #getSegmentWriteCacheKeyLimitDuringMaintenance()} for the
     * canonical name.
     *
     * @return max buffered keys inside one routed segment
     * @deprecated use
     *             {@link #getSegmentWriteCacheKeyLimitDuringMaintenance()}
     */
    @Deprecated
    public Integer getMaxNumberOfKeysInPartitionBuffer() {
        return legacyPartitionCompatibilityConfiguration
                .getMaxNumberOfKeysInPartitionBuffer();
    }

    /**
     * Returns the maximum number of buffered keys allowed across the whole
     * index.
     * <p>
     * Use {@link #getIndexBufferedWriteKeyLimit()} for the canonical name.
     *
     * @return global buffered key count
     * @deprecated use {@link #getIndexBufferedWriteKeyLimit()}
     */
    @Deprecated
    public Integer getMaxNumberOfKeysInIndexBuffer() {
        return legacyPartitionCompatibilityConfiguration
                .getMaxNumberOfKeysInIndexBuffer();
    }

    /**
     * Returns the maximum number of keys per segment chunk used for on-disk
     * layout and indexing.
     *
     * @return max keys per segment chunk
     */
    public Integer getMaxNumberOfKeysInSegmentChunk() {
        return maxNumberOfKeysInSegmentChunk;
    }

    /**
     * Returns the maximum number of delta cache files allowed before
     * maintenance is triggered.
     *
     * @return max delta cache files per segment
     */
    public Integer getMaxNumberOfDeltaCacheFiles() {
        return maxNumberOfDeltaCacheFiles;
    }

    /**
     * Returns the logical name of the index.
     *
     * @return index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the maximum number of keys allowed within a single segment.
     *
     * @return max keys per segment
     */
    public Integer getMaxNumberOfKeysInSegment() {
        return maxNumberOfKeysInSegment;
    }

    /**
     * Returns the split threshold for a routed segment.
     * <p>
     * Use {@link #getSegmentSplitKeyThreshold()} for the canonical name.
     *
     * @return max keys before a routed segment is split
     * @deprecated use {@link #getSegmentSplitKeyThreshold()}
     */
    @Deprecated
    public Integer getMaxNumberOfKeysInPartitionBeforeSplit() {
        return legacyPartitionCompatibilityConfiguration
                .getMaxNumberOfKeysInPartitionBeforeSplit();
    }

    /**
     * Returns the number of hash functions used by the Bloom filter.
     *
     * @return Bloom filter hash function count
     */
    public Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    /**
     * Returns the size of the Bloom filter index in bytes.
     *
     * @return Bloom filter size in bytes
     */
    public Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    /**
     * Returns the target false-positive probability for the Bloom filter
     * (0.0–1.0).
     *
     * @return Bloom filter false-positive probability
     */
    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    /**
     * Returns the number of threads used for segment maintenance.
     *
     * @return segment maintenance thread count
     */
    public Integer getNumberOfSegmentMaintenanceThreads() {
        return numberOfSegmentMaintenanceThreads;
    }

    /**
     * Returns the number of threads used for split maintenance.
     *
     * @return split maintenance thread count
     */
    public Integer getNumberOfIndexMaintenanceThreads() {
        return numberOfIndexMaintenanceThreads;
    }

    /**
     * Returns the number of threads used by registry lifecycle maintenance.
     *
     * @return registry lifecycle thread count
     */
    public Integer getNumberOfRegistryLifecycleThreads() {
        return numberOfRegistryLifecycleThreads;
    }

    /**
     * Returns the busy backoff delay in milliseconds for index retries.
     *
     * @return busy backoff in milliseconds
     */
    public Integer getIndexBusyBackoffMillis() {
        return indexBusyBackoffMillis;
    }

    /**
     * Returns the busy retry timeout in milliseconds for index operations.
     *
     * @return busy retry timeout in milliseconds
     */
    public Integer getIndexBusyTimeoutMillis() {
        return indexBusyTimeoutMillis;
    }

    /**
     * Returns whether auto flush/compact is scheduled after writes.
     *
     * @return true if auto maintenance is enabled; otherwise false
     */
    public Boolean isBackgroundMaintenanceAutoEnabled() {
        return backgroundMaintenanceAutoEnabled;
    }

    /**
     * Returns the maximum number of segments retained in the in-memory segment
     * cache.
     *
     * @return max segments in cache
     */
    public Integer getMaxNumberOfSegmentsInCache() {
        return maxNumberOfSegmentsInCache;
    }

    /**
     * Returns the disk I/O buffer size in bytes.
     *
     * @return disk I/O buffer size in bytes
     */
    public Integer getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    /**
     * Indicates whether logging context propagation via MDC is enabled.
     *
     * @return true if context logging is enabled; otherwise false
     */
    public Boolean isContextLoggingEnabled() {
        return contextLoggingEnabled;
    }

    /**
     * Returns WAL configuration for this index.
     *
     * @return non-null WAL configuration
     */
    public Wal getWal() {
        return wal;
    }

    /**
     * Returns the key class for this index.
     *
     * @return key class
     */
    public Class<K> getKeyClass() {
        return keyClass;
    }

    /**
     * Returns the value class for this index.
     *
     * @return value class
     */
    public Class<V> getValueClass() {
        return valueClass;
    }

    /**
     * Returns the fully qualified class name of the key type descriptor used
     * for serialization.
     *
     * @return key type descriptor class name
     */
    public String getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    /**
     * Returns the fully qualified class name of the value type descriptor used
     * for serialization.
     *
     * @return value type descriptor class name
     */
    public String getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    /**
     * Returns immutable encoding filter specs.
     *
     * @return encoding filter specs
     */
    public List<ChunkFilterSpec> getEncodingChunkFilterSpecs() {
        return encodingChunkFilters;
    }

    /**
     * Returns immutable decoding filter specs.
     *
     * @return decoding filter specs
     */
    public List<ChunkFilterSpec> getDecodingChunkFilterSpecs() {
        return decodingChunkFilters;
    }

    /**
     * Materializes encoding filters using the built-in chunk filter provider
     * registry.
     *
     * <p>
     * This is a convenience view over persisted metadata. Custom providers
     * that are not registered in the default registry must be resolved through
     * {@link #resolveRuntimeConfiguration(ChunkFilterProviderRegistry)}.
     * </p>
     *
     * @return immutable encoding filter list
     */
    public List<ChunkFilter> getEncodingChunkFilters() {
        return resolveRuntimeConfiguration().getEncodingChunkFilters();
    }

    /**
     * Materializes decoding filters using the built-in chunk filter provider
     * registry.
     *
     * <p>
     * This is a convenience view over persisted metadata. Custom providers
     * that are not registered in the default registry must be resolved through
     * {@link #resolveRuntimeConfiguration(ChunkFilterProviderRegistry)}.
     * </p>
     *
     * @return immutable decoding filter list
     */
    public List<ChunkFilter> getDecodingChunkFilters() {
        return resolveRuntimeConfiguration().getDecodingChunkFilters();
    }

    /**
     * Resolves this persisted configuration into runtime filter suppliers using
     * the built-in chunk filter provider registry.
     *
     * @return runtime configuration resolved from persisted metadata
     */
    public IndexRuntimeConfiguration<K, V> resolveRuntimeConfiguration() {
        return resolveRuntimeConfiguration(
                ChunkFilterProviderRegistry.defaultRegistry());
    }

    /**
     * Resolves this persisted configuration into runtime filter suppliers using
     * the provided chunk filter provider registry.
     *
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return runtime configuration resolved from persisted metadata
     */
    public IndexRuntimeConfiguration<K, V> resolveRuntimeConfiguration(
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return IndexRuntimeConfiguration.resolve(this,
                chunkFilterProviderRegistry);
    }
}
