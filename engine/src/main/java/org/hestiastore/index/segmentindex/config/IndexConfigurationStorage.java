package org.hestiastore.index.segmentindex.config;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterBuilder;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyView;
import org.hestiastore.index.properties.PropertyWriter;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;

/**
 * Persists {@link IndexConfiguration} instances to the index configuration
 * property store.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexConfigurationStorage<K, V> {

    private static final IndexPropertiesSchema SCHEMA = IndexPropertiesSchema.INDEX_CONFIGURATION_SCHEMA;
    private static final String PROP_KEY_CLASS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS;
    private static final String PROP_VALUE_CLASS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS;
    private static final String PROP_KEY_TYPE_DESCRIPTOR = IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR;
    private static final String PROP_VALUE_TYPE_DESCRIPTOR = IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR;
    private static final String PROP_INDEX_NAME = IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME;
    private static final String PROP_CONTEXT_LOGGING_ENABLED = IndexPropertiesSchema.IndexConfigurationKeys.PROP_CONTEXT_LOGGING_ENABLED;

    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION;
    private static final String PROP_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK;
    private static final String PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT;
    private static final String PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE;
    private static final String PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS;
    private static final String PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS;
    private static final String PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS;
    private static final String PROP_INDEX_BUSY_BACKOFF_MILLIS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_BUSY_BACKOFF_MILLIS;
    private static final String PROP_INDEX_BUSY_TIMEOUT_MILLIS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_BUSY_TIMEOUT_MILLIS;
    private static final String PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED = IndexPropertiesSchema.IndexConfigurationKeys.PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED;
    private static final String PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    private static final String PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    private static final String PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = IndexPropertiesSchema.IndexConfigurationKeys.PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE;
    private static final String PROP_DISK_IO_BUFFER_SIZE_IN_BYTES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_DISK_IO_BUFFER_SIZE_IN_BYTES;
    private static final String PROP_ENCODING_CHUNK_FILTERS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS;
    private static final String PROP_DECODING_CHUNK_FILTERS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS;
    private static final String PROP_WAL_ENABLED = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_ENABLED;
    private static final String PROP_WAL_DURABILITY_MODE = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_DURABILITY_MODE;
    private static final String PROP_WAL_SEGMENT_SIZE_BYTES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_SEGMENT_SIZE_BYTES;
    private static final String PROP_WAL_GROUP_SYNC_DELAY_MILLIS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_DELAY_MILLIS;
    private static final String PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES;
    private static final String PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT;
    private static final String PROP_WAL_CORRUPTION_POLICY = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_CORRUPTION_POLICY;
    private static final String PROP_WAL_EPOCH_SUPPORT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_EPOCH_SUPPORT;
    private static final String LEGACY_PROP_SEGMENT_MAINTENANCE_AUTO_ENABLED = "segmentMaintenanceAutoEnabled";
    private static final String LEGACY_PROP_SEGMENT_INDEX_MAINTENANCE_THREADS = "segmentIndexMaintenanceThreads";
    private static final String LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE = "maxNumberOfKeysInSegmentWriteCache";
    private static final String LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE = "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance";

    private static final String CONFIGURATION_FILENAME = IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME;

    private final Directory directoryFacade;

    public IndexConfigurationStorage(final Directory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
    }

    /**
     * Creates a configuration storage wrapper.
     *
     * <p>
     * The chunk filter provider resolver parameter is retained for source and
     * binary compatibility with earlier runtime-coupled storage code. Storage
     * now persists only {@code ChunkFilterSpec} metadata and no longer resolves
     * runtime suppliers while loading.
     * </p>
     *
     * @param directoryFacade backing directory
     * @param chunkFilterProviderResolver ignored runtime resolver retained for
     *                                    compatibility
     */
    public IndexConfigurationStorage(final Directory directoryFacade,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        this(directoryFacade);
        Vldtn.requireNonNull(chunkFilterProviderResolver,
                "chunkFilterProviderResolver");
    }

    public IndexConfiguration<K, V> load() {
        final PropertyStore props = PropertyStoreImpl
                .fromDirectory(directoryFacade, CONFIGURATION_FILENAME, true);
        SCHEMA.ensure(props);
        final PropertyView propsView = props.snapshot();
        final Class<K> keyClass = toClass(propsView.getString(PROP_KEY_CLASS));
        final Class<V> valueClass = toClass(
                propsView.getString(PROP_VALUE_CLASS));
        final long maxNumberOfKeysInSegmentCache = propsView
                .getLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        final long defaultMaxNumberOfKeysInActivePartition = maxNumberOfKeysInSegmentCache > 0
                ? maxNumberOfKeysInSegmentCache / 2
                : IndexConfigurationContract.DEFAULT_SEGMENT_CACHE_KEY_LIMIT
                        / 2;
        final long maxNumberOfKeysInActivePartition = getOrDefaultLong(
                propsView, PROP_MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                defaultMaxNumberOfKeysInActivePartition);
        final long defaultMaxNumberOfKeysInPartitionBuffer = Math.max(
                maxNumberOfKeysInActivePartition * 2,
                maxNumberOfKeysInActivePartition + 1);
        final long maxNumberOfKeysInPartitionBuffer = getOrDefaultLong(propsView,
                PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                defaultMaxNumberOfKeysInPartitionBuffer);
        final int maxNumberOfImmutableRunsPerPartition = getOrDefault(propsView,
                PROP_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
                IndexConfigurationContract.DEFAULT_LEGACY_IMMUTABLE_RUN_LIMIT);
        final long maxNumberOfKeysInIndexBuffer = getOrDefaultLong(propsView,
                PROP_MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                Math.max(maxNumberOfKeysInPartitionBuffer,
                        maxNumberOfKeysInPartitionBuffer
                                * Math.max(1L,
                                        propsView.getInt(
                                                PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE))));
        final int maxNumberOfDeltaCacheFiles = getOrDefault(propsView,
                PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES,
                IndexConfigurationContract.DEFAULT_DELTA_CACHE_FILE_LIMIT);
        final int maxNumberOfKeysInPartitionBeforeSplit = getOrDefault(propsView,
                PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                IndexConfigurationContract.DEFAULT_SEGMENT_SPLIT_KEY_THRESHOLD);
        final int maxNumberOfKeysInSegment = getOrDefault(propsView,
                PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                maxNumberOfKeysInPartitionBeforeSplit);
        final IndexConfigurationBuilder<K, V> builder = IndexConfiguration
                .<K, V>builder()
                .identity(identity -> identity.keyClass(keyClass)
                        .valueClass(valueClass)
                        .name(propsView.getString(PROP_INDEX_NAME))
                        .keyTypeDescriptor(
                                propsView.getString(PROP_KEY_TYPE_DESCRIPTOR))
                        .valueTypeDescriptor(propsView
                                .getString(PROP_VALUE_TYPE_DESCRIPTOR)))
                .logging(logging -> logging.contextEnabled(
                        propsView.getBoolean(PROP_CONTEXT_LOGGING_ENABLED)))
                .segment(segment -> segment
                        .cachedSegmentLimit(propsView
                                .getInt(PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE))
                        .maxKeys(maxNumberOfKeysInSegment)
                        .cacheKeyLimit((int) maxNumberOfKeysInSegmentCache)
                        .chunkKeyLimit(propsView.getInt(
                                PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK))
                        .deltaCacheFileLimit(maxNumberOfDeltaCacheFiles))
                .writePath(writePath -> writePath
                        .segmentSplitKeyThreshold(
                                maxNumberOfKeysInPartitionBeforeSplit)
                        .segmentWriteCacheKeyLimit(
                                (int) maxNumberOfKeysInActivePartition)
                        .legacyImmutableRunLimit(
                                maxNumberOfImmutableRunsPerPartition)
                        .maintenanceWriteCacheKeyLimit(
                                (int) maxNumberOfKeysInPartitionBuffer)
                        .indexBufferedWriteKeyLimit(
                                (int) maxNumberOfKeysInIndexBuffer))
                .io(io -> io.diskBufferSizeBytes(
                        propsView.getInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES)))
                .maintenance(maintenance -> maintenance
                        .segmentThreads(getOrDefault(propsView,
                                PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS,
                                IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS))
                        .indexThreads(getOrDefault(propsView,
                                PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS,
                                IndexConfigurationContract.DEFAULT_INDEX_MAINTENANCE_THREADS))
                        .registryLifecycleThreads(getOrDefault(propsView,
                                PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS,
                                IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS))
                        .busyBackoffMillis(getOrDefault(propsView,
                                PROP_INDEX_BUSY_BACKOFF_MILLIS,
                                IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS))
                        .busyTimeoutMillis(getOrDefault(propsView,
                                PROP_INDEX_BUSY_TIMEOUT_MILLIS,
                                IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS))
                        .backgroundAutoEnabled(getOrDefaultBoolean(propsView,
                                PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED,
                                IndexConfigurationContract.DEFAULT_BACKGROUND_MAINTENANCE_AUTO_ENABLED)))
                .bloomFilter(bloomFilter -> bloomFilter
                        .hashFunctions(propsView.getInt(
                                PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS))
                        .indexSizeBytes(propsView
                                .getInt(PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES)));

        if (propsView.getDouble(
                PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE) != 0) {
            builder.bloomFilter(bloomFilter -> bloomFilter
                    .falsePositiveProbability(propsView.getDouble(
                            PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE)));
        }

        final String encodingFilters = propsView
                .getString(PROP_ENCODING_CHUNK_FILTERS);
        if (encodingFilters != null && !encodingFilters.isBlank()) {
            builder.filters(filters -> filters.encodingFilterSpecs(
                    ChunkFilterSpecCodec.parse(encodingFilters)));
        }

        final String decodingFilters = propsView
                .getString(PROP_DECODING_CHUNK_FILTERS);
        if (decodingFilters != null && !decodingFilters.isBlank()) {
            builder.filters(filters -> filters.decodingFilterSpecs(
                    ChunkFilterSpecCodec.parse(decodingFilters)));
        }

        final boolean walEnabled = getOrDefaultBoolean(propsView,
                PROP_WAL_ENABLED, false);
        if (walEnabled) {
            builder.wal(wal -> wal
                    .durability(resolveEnum(propsView,
                            PROP_WAL_DURABILITY_MODE,
                            IndexWalConfiguration.DEFAULT_DURABILITY_MODE,
                            WalDurabilityMode.class))
                    .segmentSizeBytes(getOrDefaultLong(propsView,
                            PROP_WAL_SEGMENT_SIZE_BYTES,
                            IndexWalConfiguration.DEFAULT_SEGMENT_SIZE_BYTES))
                    .groupSyncDelayMillis(getOrDefault(propsView,
                            PROP_WAL_GROUP_SYNC_DELAY_MILLIS,
                            IndexWalConfiguration.DEFAULT_GROUP_SYNC_DELAY_MILLIS))
                    .groupSyncMaxBatchBytes(getOrDefault(propsView,
                            PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES,
                            IndexWalConfiguration.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES))
                    .maxBytesBeforeForcedCheckpoint(getOrDefaultLong(propsView,
                            PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                            IndexWalConfiguration.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT))
                    .corruptionPolicy(resolveEnum(propsView,
                            PROP_WAL_CORRUPTION_POLICY,
                            IndexWalConfiguration.DEFAULT_CORRUPTION_POLICY,
                            WalCorruptionPolicy.class))
                    .epochSupport(getOrDefaultBoolean(propsView,
                            PROP_WAL_EPOCH_SUPPORT, false)));
        } else {
            builder.wal(wal -> wal.disabled());
        }

        return builder.build();
    }

    /**
     * Persists the provided configuration to the backing property store.
     *
     * @param indexConfiguration configuration to persist
     */
    public void save(IndexConfiguration<K, V> indexConfiguration) {
        final var identity = indexConfiguration.identity();
        final var logging = indexConfiguration.logging();
        final var segment = indexConfiguration.segment();
        final var writePath = indexConfiguration.writePath();
        final var tuning = indexConfiguration.runtimeTuning();
        final var maintenance = indexConfiguration.maintenance();
        final var bloomFilter = indexConfiguration.bloomFilter();
        final var filters = indexConfiguration.filters();
        final PropertyStore props = PropertyStoreImpl
                .fromDirectory(directoryFacade, CONFIGURATION_FILENAME, false);
        final PropertyTransaction tx = props.beginTransaction();
        final PropertyWriter writer = tx.openPropertyWriter();
        writer.setString(PROP_KEY_CLASS, identity.keyClass().getName());
        writer.setString(PROP_VALUE_CLASS, identity.valueClass().getName());
        writer.setString(PROP_KEY_TYPE_DESCRIPTOR,
                identity.keyTypeDescriptor());
        writer.setString(PROP_VALUE_TYPE_DESCRIPTOR,
                identity.valueTypeDescriptor());
        writer.setString(PROP_INDEX_NAME, identity.name());
        writer.setBoolean(PROP_CONTEXT_LOGGING_ENABLED,
                logging.contextEnabled());

        // SegmentIndex runtime properties
        writer.setInt(PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                segment.cachedSegmentLimit());
        writer.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                segment.maxKeys());
        writer.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                writePath.segmentSplitKeyThreshold());
        writer.setInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES,
                indexConfiguration.io().diskBufferSizeBytes());

        // Segment properties
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                segment.cacheKeyLimit());
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                writePath.segmentWriteCacheKeyLimit());
        writer.setInt(PROP_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
                tuning.legacyImmutableRunLimit());
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                writePath.segmentWriteCacheKeyLimitDuringMaintenance());
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                writePath.indexBufferedWriteKeyLimit());
        writer.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK,
                segment.chunkKeyLimit());
        final int deltaCacheFileCount = segment
                .deltaCacheFileLimit() == null
                        ? IndexConfigurationContract.DEFAULT_DELTA_CACHE_FILE_LIMIT
                        : segment.deltaCacheFileLimit();
        writer.setInt(PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES,
                deltaCacheFileCount);
        final int maintenanceThreads = maintenance
                .segmentThreads() == null
                        ? IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS
                        : maintenance.segmentThreads();
        writer.setInt(PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS,
                maintenanceThreads);
        final int indexMaintenanceThreads = maintenance
                .indexThreads() == null
                        ? IndexConfigurationContract.DEFAULT_INDEX_MAINTENANCE_THREADS
                        : maintenance.indexThreads();
        writer.setInt(PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS,
                indexMaintenanceThreads);
        final int registryLifecycleThreads = maintenance
                .registryLifecycleThreads() == null
                        ? IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS
                        : maintenance.registryLifecycleThreads();
        writer.setInt(PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS,
                registryLifecycleThreads);
        final int busyBackoffMillis = maintenance
                .busyBackoffMillis() == null
                        ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS
                        : maintenance.busyBackoffMillis();
        writer.setInt(PROP_INDEX_BUSY_BACKOFF_MILLIS, busyBackoffMillis);
        final int busyTimeoutMillis = maintenance
                .busyTimeoutMillis() == null
                        ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS
                        : maintenance.busyTimeoutMillis();
        writer.setInt(PROP_INDEX_BUSY_TIMEOUT_MILLIS, busyTimeoutMillis);
        writer.setBoolean(PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED, Boolean.TRUE
                .equals(maintenance.backgroundAutoEnabled()));
        // Segment bloom filter properties
        writer.setInt(PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                bloomFilter.hashFunctions());
        writer.setInt(PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES,
                bloomFilter.indexSizeBytes());
        if (bloomFilter.falsePositiveProbability() != null) {
            writer.setDouble(PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                    bloomFilter.falsePositiveProbability());
        } else {
            writer.setDouble(PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                    BloomFilterBuilder.DEFAULT_PROBABILITY_OF_FALSE_POSITIVE);
        }
        writer.setString(PROP_ENCODING_CHUNK_FILTERS,
                ChunkFilterSpecCodec
                        .serialize(filters.encodingChunkFilterSpecs()));

        writer.setString(PROP_DECODING_CHUNK_FILTERS,
                ChunkFilterSpecCodec
                        .serialize(filters.decodingChunkFilterSpecs()));
        final IndexWalConfiguration wal = IndexWalConfiguration.orEmpty(indexConfiguration.wal());
        writer.setBoolean(PROP_WAL_ENABLED, wal.isEnabled());
        writer.setString(PROP_WAL_DURABILITY_MODE,
                wal.getDurabilityMode().name());
        writer.setLong(PROP_WAL_SEGMENT_SIZE_BYTES, wal.getSegmentSizeBytes());
        writer.setInt(PROP_WAL_GROUP_SYNC_DELAY_MILLIS,
                wal.getGroupSyncDelayMillis());
        writer.setInt(PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES,
                wal.getGroupSyncMaxBatchBytes());
        writer.setLong(PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                wal.getMaxBytesBeforeForcedCheckpoint());
        writer.setString(PROP_WAL_CORRUPTION_POLICY,
                wal.getCorruptionPolicy().name());
        writer.setBoolean(PROP_WAL_EPOCH_SUPPORT, wal.isEpochSupport());
        writer.remove(LEGACY_PROP_SEGMENT_MAINTENANCE_AUTO_ENABLED);
        writer.remove(LEGACY_PROP_SEGMENT_INDEX_MAINTENANCE_THREADS);
        writer.remove(LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE);
        writer.remove(
                LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE);
        SCHEMA.writeMetadata(writer);
        tx.close();
    }

    public boolean exists() {
        return directoryFacade.isFileExists(CONFIGURATION_FILENAME);
    }

    @SuppressWarnings("unchecked")
    private <T> Class<T> toClass(final String className) {
        try {
            return (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException(
                    "Unable to load class: " + className, ex);
        }
    }

    private int getOrDefault(final PropertyView propsView, final String key,
            final int defaultValue) {
        final int value = propsView.getInt(key);
        if (value == 0) {
            return defaultValue;
        }
        return value;
    }

    private long getOrDefaultLong(final PropertyView propsView,
            final String key, final long defaultValue) {
        final long value = propsView.getLong(key);
        if (value == 0) {
            return defaultValue;
        }
        return value;
    }

    private boolean getOrDefaultBoolean(final PropertyView propsView,
            final String key, final boolean defaultValue) {
        final String value = propsView.getString(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private <T extends Enum<T>> T resolveEnum(final PropertyView propsView,
            final String key, final T defaultValue, final Class<T> enumClass) {
        final String value = propsView.getString(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Enum.valueOf(enumClass, value.trim());
        } catch (final IllegalArgumentException ex) {
            return defaultValue;
        }
    }

}
