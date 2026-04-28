package org.hestiastore.index.segmentindex;

import java.util.List;

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
    private final Integer legacyImmutableRunLimit;

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
        this.legacyImmutableRunLimit = maxNumberOfImmutableRunsPerPartition;
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
     * Returns grouped identity and key/value type metadata.
     *
     * @return immutable identity view
     */
    public IndexIdentityConfiguration<K, V> identity() {
        return new IndexIdentityConfiguration<>(indexName, keyClass, valueClass,
                keyTypeDescriptor, valueTypeDescriptor);
    }

    /**
     * Returns grouped segment sizing and cache settings.
     *
     * @return immutable segment settings view
     */
    public IndexSegmentConfiguration segment() {
        return new IndexSegmentConfiguration(maxNumberOfKeysInSegment,
                maxNumberOfKeysInSegmentChunk,
                maxNumberOfKeysInSegmentCache, maxNumberOfSegmentsInCache,
                maxNumberOfDeltaCacheFiles);
    }

    /**
     * Returns canonical direct-to-segment write-path settings.
     *
     * @return immutable write-path settings
     */
    public IndexWritePathConfiguration writePath() {
        return writePathConfiguration;
    }

    /**
     * Returns grouped Bloom filter settings.
     *
     * @return immutable Bloom filter settings view
     */
    public IndexBloomFilterConfiguration bloomFilter() {
        return new IndexBloomFilterConfiguration(
                bloomFilterNumberOfHashFunctions,
                bloomFilterIndexSizeInBytes,
                bloomFilterProbabilityOfFalsePositive);
    }

    /**
     * Returns grouped maintenance and retry settings.
     *
     * @return immutable maintenance settings view
     */
    public IndexMaintenanceConfiguration maintenance() {
        return new IndexMaintenanceConfiguration(
                numberOfSegmentMaintenanceThreads,
                numberOfIndexMaintenanceThreads,
                numberOfRegistryLifecycleThreads, indexBusyBackoffMillis,
                indexBusyTimeoutMillis, backgroundMaintenanceAutoEnabled);
    }

    /**
     * Returns grouped disk I/O settings.
     *
     * @return immutable I/O settings view
     */
    public IndexIoConfiguration io() {
        return new IndexIoConfiguration(diskIoBufferSize);
    }

    /**
     * Returns grouped logging settings.
     *
     * @return immutable logging settings view
     */
    public IndexLoggingConfiguration logging() {
        return new IndexLoggingConfiguration(contextLoggingEnabled);
    }

    /**
     * Returns WAL settings.
     *
     * @return non-null WAL settings
     */
    public Wal wal() {
        return wal;
    }

    /**
     * Returns grouped persisted chunk filter settings.
     *
     * @return immutable filter settings view
     */
    public IndexFilterConfiguration filters() {
        return new IndexFilterConfiguration(encodingChunkFilters,
                decodingChunkFilters);
    }

    /**
     * Returns grouped runtime-tunable settings derived from this
     * configuration.
     *
     * @return immutable runtime tuning settings view
     */
    public IndexRuntimeTuningConfiguration runtimeTuning() {
        return new IndexRuntimeTuningConfiguration(maxNumberOfSegmentsInCache,
                maxNumberOfKeysInSegmentCache, writePathConfiguration,
                legacyImmutableRunLimit);
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
