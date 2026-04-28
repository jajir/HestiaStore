package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
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
    private Integer numberOfSegmentMaintenanceThreads;
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
    private final List<ChunkFilterSpec> encodingChunkFilters = new ArrayList<>();
    private final List<ChunkFilterSpec> decodingChunkFilters = new ArrayList<>();

    IndexConfigurationBuilder() {

    }

    /**
     * Configures index identity and type metadata.
     *
     * @param customizer identity section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> identity(
            final Consumer<IndexIdentityConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexIdentityConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Configures segment sizing and cache settings.
     *
     * @param customizer segment section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> segment(
            final Consumer<IndexSegmentConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexSegmentConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Configures direct-to-segment write path settings.
     *
     * @param customizer write-path section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> writePath(
            final Consumer<IndexWritePathConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexWritePathConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Configures Bloom filter settings.
     *
     * @param customizer Bloom filter section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> bloomFilter(
            final Consumer<IndexBloomFilterConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexBloomFilterConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Configures WAL settings.
     *
     * @param customizer WAL section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> wal(
            final Consumer<IndexWalConfigurationBuilder<K, V>> customizer) {
        final IndexWalConfigurationBuilder<K, V> section =
                new IndexWalConfigurationBuilder<>();
        Vldtn.requireNonNull(customizer, "customizer").accept(section);
        section.applyTo(this);
        return this;
    }

    /**
     * Configures maintenance and retry settings.
     *
     * @param customizer maintenance section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> maintenance(
            final Consumer<IndexMaintenanceConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexMaintenanceConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Configures I/O settings.
     *
     * @param customizer I/O section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> io(
            final Consumer<IndexIoConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexIoConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Configures logging settings.
     *
     * @param customizer logging section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> logging(
            final Consumer<IndexLoggingConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexLoggingConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Configures persisted chunk filter pipelines.
     *
     * @param customizer filter section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> filters(
            final Consumer<IndexFilterConfigurationBuilder<K, V>> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new IndexFilterConfigurationBuilder<>(this));
        return this;
    }

    /**
     * Sets the key type descriptor instance used for serialization.
     *
     * @param keyTypeDescriptor type descriptor for keys
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setKeyTypeDescriptor(
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
    IndexConfigurationBuilder<K, V> setValueTypeDescriptor(
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
    IndexConfigurationBuilder<K, V> setKeyTypeDescriptor(
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
    IndexConfigurationBuilder<K, V> setValueTypeDescriptor(
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
    IndexConfigurationBuilder<K, V> setKeyClass(
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
    IndexConfigurationBuilder<K, V> setValueClass(
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
    IndexConfigurationBuilder<K, V> setName(final String indexName) {
        this.indexName = indexName;
        return this;
    }

    /**
     * Sets the max number of keys held in the in-memory segment cache.
     *
     * @param maxNumberOfKeysInSegmentCache max keys in segment cache
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setSegmentCacheKeyLimit(
            final Integer maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        return this;
    }

    /**
     * Sets the maximum number of keys accepted into one routed segment write
     * cache.
     *
     * @param segmentWriteCacheKeyLimit max keys in one segment write cache
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setSegmentWriteCacheKeyLimit(
            final Integer segmentWriteCacheKeyLimit) {
        this.maxNumberOfKeysInActivePartition = segmentWriteCacheKeyLimit;
        return this;
    }

    /**
     * Sets the max number of keys per on-disk segment chunk.
     *
     * @param maxNumberOfKeysInSegmentChunk max keys per chunk
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setSegmentChunkKeyLimit(
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
    IndexConfigurationBuilder<K, V> setSegmentDeltaCacheFileLimit(
            final Integer maxNumberOfDeltaCacheFiles) {
        this.maxNumberOfDeltaCacheFiles = maxNumberOfDeltaCacheFiles;
        return this;
    }

    /**
     * Sets a legacy compatibility limit retained from the removed partition
     * runtime.
     *
     * @param maxNumberOfImmutableRunsPerPartition legacy compatibility limit
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setLegacyImmutableRunLimit(
            final Integer maxNumberOfImmutableRunsPerPartition) {
        this.maxNumberOfImmutableRunsPerPartition = maxNumberOfImmutableRunsPerPartition;
        return this;
    }

    /**
     * Sets the maximum number of keys buffered inside one routed segment while
     * maintenance is running.
     *
     * @param segmentWriteCacheKeyLimitDuringMaintenance per-segment
     *        maintenance-time buffered key count
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setSegmentWriteCacheKeyLimitDuringMaintenance(
            final Integer segmentWriteCacheKeyLimitDuringMaintenance) {
        this.maxNumberOfKeysInPartitionBuffer = segmentWriteCacheKeyLimitDuringMaintenance;
        return this;
    }

    /**
     * Sets the max number of keys allowed within a segment.
     *
     * @param maxNumberOfKeysInSegment max keys per segment
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setSegmentMaxKeys(
            final Integer maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        return this;
    }

    /**
     * Sets the maximum number of keys buffered across the whole index.
     *
     * @param indexBufferedWriteKeyLimit global buffered key count
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setIndexBufferedWriteKeyLimit(
            final Integer indexBufferedWriteKeyLimit) {
        this.maxNumberOfKeysInIndexBuffer = indexBufferedWriteKeyLimit;
        return this;
    }

    /**
     * Sets the threshold at which one routed segment becomes eligible for
     * split.
     *
     * @param segmentSplitKeyThreshold max keys before split
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setSegmentSplitKeyThreshold(
            final Integer segmentSplitKeyThreshold) {
        this.maxNumberOfKeysInPartitionBeforeSplit = segmentSplitKeyThreshold;
        return this;
    }

    /**
     * Sets the max number of segments retained in the in-memory cache.
     *
     * @param maxNumberOfSegmentsInCache max segments in cache
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setCachedSegmentLimit(
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
    IndexConfigurationBuilder<K, V> setBloomFilterHashFunctionCount(
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
    IndexConfigurationBuilder<K, V> setBloomFilterFalsePositiveProbability(
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
    IndexConfigurationBuilder<K, V> setBloomFilterIndexSizeBytes(
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
    IndexConfigurationBuilder<K, V> setDiskIoBufferSizeBytes(
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
    IndexConfigurationBuilder<K, V> setContextLoggingEnabled(
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
    IndexConfigurationBuilder<K, V> setWal(final Wal wal) {
        this.wal = Wal.orEmpty(wal);
        return this;
    }

    /**
     * Sets the number of segment maintenance threads.
     *
     * @param numberOfSegmentMaintenanceThreads segment maintenance thread count
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setSegmentMaintenanceThreadCount(
            final Integer numberOfSegmentMaintenanceThreads) {
        this.numberOfSegmentMaintenanceThreads = numberOfSegmentMaintenanceThreads;
        return this;
    }

    /**
     * Sets the number of split maintenance threads.
     *
     * @param numberOfIndexMaintenanceThreads split maintenance thread count
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setIndexMaintenanceThreadCount(
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
    IndexConfigurationBuilder<K, V> setRegistryLifecycleThreadCount(
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
    IndexConfigurationBuilder<K, V> setBusyBackoffMillis(
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
    IndexConfigurationBuilder<K, V> setBusyTimeoutMillis(
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
    IndexConfigurationBuilder<K, V> setBackgroundMaintenanceAutoEnabled(
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
    IndexConfigurationBuilder<K, V> addEncodingFilter(
            final ChunkFilter filter) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(filter,
                "filter");
        return addEncodingFilter(() -> requiredFilter,
                resolvePersistableInstanceSpec(requiredFilter, true));
    }

    /**
     * Adds a chunk filter class to the encoding pipeline.
     *
     * @param filterClass filter class to instantiate
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> addEncodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(filterClass, "filterClass");
        return addEncodingFilter(() -> instantiateFilter(requiredClass),
                ChunkFilterSpecs.forEncodingFilter(requiredClass));
    }

    /**
     * Adds an encoding filter described by persisted metadata.
     *
     * <p>
     * The supplier parameter is accepted for compatibility with existing
     * builder call sites, but {@link IndexConfiguration} persists only the
     * provided {@link ChunkFilterSpec}. Runtime suppliers are resolved later
     * through {@link IndexRuntimeConfiguration}.
     * </p>
     *
     * @param supplier runtime filter supplier
     * @param spec persisted filter spec
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> addEncodingFilter(
            final Supplier<? extends ChunkFilter> supplier,
            final ChunkFilterSpec spec) {
        Vldtn.requireNonNull(supplier, "supplier");
        return addEncodingFilter(spec);
    }

    /**
     * Adds an encoding filter descriptor to the persisted filter pipeline.
     *
     * @param spec persisted filter spec
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> addEncodingFilter(
            final ChunkFilterSpec spec) {
        return addEncodingFilterSpec(Vldtn.requireNonNull(spec, "spec"));
    }

    /**
     * Replaces the encoding filter pipeline with the supplied registrations.
     *
     * @param registrations filter registrations to use
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setEncodingFilterRegistrations(
            final Collection<ChunkFilterRegistration> registrations) {
        Vldtn.requireNonNull(registrations, "registrations");
        encodingChunkFilters.clear();
        for (final ChunkFilterRegistration registration : registrations) {
            addEncodingFilterSpec(Vldtn.requireNonNull(registration,
                    "registration").getSpec());
        }
        return this;
    }

    /**
     * Replaces the encoding filter pipeline with the supplied specs.
     *
     * @param specs filter specs to use
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setEncodingFilterSpecs(
            final Collection<ChunkFilterSpec> specs) {
        Vldtn.requireNonNull(specs, "specs");
        encodingChunkFilters.clear();
        for (final ChunkFilterSpec spec : specs) {
            addEncodingFilterSpec(spec);
        }
        return this;
    }

    /**
     * Replaces the encoding filter pipeline with the supplied filter classes.
     *
     * @param filterClasses filter classes to instantiate
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setEncodingFilterClasses(
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
    IndexConfigurationBuilder<K, V> setEncodingFilters(
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
    IndexConfigurationBuilder<K, V> addDecodingFilter(
            final ChunkFilter filter) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(filter,
                "filter");
        return addDecodingFilter(() -> requiredFilter,
                resolvePersistableInstanceSpec(requiredFilter, false));
    }

    /**
     * Adds a chunk filter class to the decoding pipeline.
     *
     * @param filterClass filter class to instantiate
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> addDecodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(filterClass, "filterClass");
        return addDecodingFilter(() -> instantiateFilter(requiredClass),
                ChunkFilterSpecs.forDecodingFilter(requiredClass));
    }

    /**
     * Adds a decoding filter described by persisted metadata.
     *
     * <p>
     * The supplier parameter is accepted for compatibility with existing
     * builder call sites, but {@link IndexConfiguration} persists only the
     * provided {@link ChunkFilterSpec}. Runtime suppliers are resolved later
     * through {@link IndexRuntimeConfiguration}.
     * </p>
     *
     * @param supplier runtime filter supplier
     * @param spec persisted filter spec
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> addDecodingFilter(
            final Supplier<? extends ChunkFilter> supplier,
            final ChunkFilterSpec spec) {
        Vldtn.requireNonNull(supplier, "supplier");
        return addDecodingFilter(spec);
    }

    /**
     * Adds a decoding filter descriptor to the persisted filter pipeline.
     *
     * @param spec persisted filter spec
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> addDecodingFilter(
            final ChunkFilterSpec spec) {
        return addDecodingFilterSpec(Vldtn.requireNonNull(spec, "spec"));
    }

    /**
     * Replaces the decoding filter pipeline with the supplied registrations.
     *
     * @param registrations filter registrations to use
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setDecodingFilterRegistrations(
            final Collection<ChunkFilterRegistration> registrations) {
        Vldtn.requireNonNull(registrations, "registrations");
        decodingChunkFilters.clear();
        for (final ChunkFilterRegistration registration : registrations) {
            addDecodingFilterSpec(Vldtn.requireNonNull(registration,
                    "registration").getSpec());
        }
        return this;
    }

    /**
     * Replaces the decoding filter pipeline with the supplied specs.
     *
     * @param specs filter specs to use
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setDecodingFilterSpecs(
            final Collection<ChunkFilterSpec> specs) {
        Vldtn.requireNonNull(specs, "specs");
        decodingChunkFilters.clear();
        for (final ChunkFilterSpec spec : specs) {
            addDecodingFilterSpec(spec);
        }
        return this;
    }

    /**
     * Replaces the decoding filter pipeline with the supplied filter classes.
     *
     * @param filterClasses filter classes to instantiate
     * @return this builder
     */
    IndexConfigurationBuilder<K, V> setDecodingFilterClasses(
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
    IndexConfigurationBuilder<K, V> setDecodingFilters(
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
        final Integer effectiveSegmentMaintenanceThreads = numberOfSegmentMaintenanceThreads == null
                ? IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS
                : numberOfSegmentMaintenanceThreads;
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
                ? IndexConfigurationContract.DEFAULT_DELTA_CACHE_FILE_LIMIT
                : maxNumberOfDeltaCacheFiles;
        final Integer effectiveMaxNumberOfImmutableRunsPerPartition = maxNumberOfImmutableRunsPerPartition == null
                ? IndexConfigurationContract.DEFAULT_LEGACY_IMMUTABLE_RUN_LIMIT
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
                contextLoggingEnabled, effectiveSegmentMaintenanceThreads,
                effectiveIndexMaintenanceThreads,
                effectiveRegistryLifecycleThreads,
                effectiveIndexBusyBackoffMillis,
                effectiveIndexBusyTimeoutMillis,
                effectiveBackgroundMaintenanceAutoEnabled,
                Wal.orEmpty(wal),
                encodingChunkFilters, decodingChunkFilters);
    }

    private IndexConfigurationBuilder<K, V> addEncodingFilterSpec(
            final ChunkFilterSpec spec) {
        encodingChunkFilters.add(Vldtn.requireNonNull(spec, "spec"));
        return this;
    }

    private IndexConfigurationBuilder<K, V> addDecodingFilterSpec(
            final ChunkFilterSpec spec) {
        decodingChunkFilters.add(Vldtn.requireNonNull(spec, "spec"));
        return this;
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
                ? IndexConfigurationContract.DEFAULT_CACHED_SEGMENT_LIMIT
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

    private ChunkFilterSpec resolvePersistableInstanceSpec(
            final ChunkFilter filter, final boolean encoding) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(filter,
                "filter");
        final ChunkFilterSpec spec = encoding
                ? ChunkFilterSpecs.forEncodingFilter(requiredFilter)
                : ChunkFilterSpecs.forDecodingFilter(requiredFilter);
        if (ChunkFilterProviderRegistry.PROVIDER_ID_JAVA_CLASS
                .equals(spec.getProviderId())) {
            throw new IllegalArgumentException(String.format(
                    "Custom %s chunk filter instances require explicit persisted metadata. "
                            + "Use %s(Supplier<? extends ChunkFilter>, ChunkFilterSpec) "
                            + "or %s(Class<? extends ChunkFilter>) for no-arg filters.",
                    encoding ? "encoding" : "decoding",
                    encoding ? "addEncodingFilter" : "addDecodingFilter",
                    encoding ? "addEncodingFilter" : "addDecodingFilter"));
        }
        return spec;
    }

}
