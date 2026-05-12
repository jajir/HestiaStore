package org.hestiastore.index.segmentindex.configuration.persistence;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterBuilder;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyView;
import org.hestiastore.index.properties.PropertyWriter;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexBloomFilterConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexChunkStoreCacheConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexFilterConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexIdentityConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexIoConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexLoggingConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexSegmentConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexWritePathConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;

/**
 * Persists {@link EffectiveIndexConfiguration} instances to the index
 * configuration property store.
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
    private static final String PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT;
    private static final String PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE = IndexPropertiesSchema.IndexConfigurationKeys.PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE;
    private static final String PROP_INDEX_BUFFERED_WRITE_KEY_LIMIT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_BUFFERED_WRITE_KEY_LIMIT;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK;
    private static final String PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES;
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT;
    private static final String PROP_SEGMENT_SPLIT_KEY_THRESHOLD = IndexPropertiesSchema.IndexConfigurationKeys.PROP_SEGMENT_SPLIT_KEY_THRESHOLD;
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
    private static final String PROP_CHUNK_STORE_CACHE_PAGE_LIMIT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_CHUNK_STORE_CACHE_PAGE_LIMIT;
    private static final String PROP_WAL_ENABLED = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_ENABLED;
    private static final String PROP_WAL_DURABILITY_MODE = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_DURABILITY_MODE;
    private static final String PROP_WAL_SEGMENT_SIZE_BYTES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_SEGMENT_SIZE_BYTES;
    private static final String PROP_WAL_GROUP_SYNC_DELAY_MILLIS = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_DELAY_MILLIS;
    private static final String PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES;
    private static final String PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT;
    private static final String PROP_WAL_CORRUPTION_POLICY = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_CORRUPTION_POLICY;
    private static final String PROP_WAL_EPOCH_SUPPORT = IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_EPOCH_SUPPORT;

    private static final String CONFIGURATION_FILENAME = IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME;

    private final Directory directoryFacade;
    private final ChunkFilterProviderResolver chunkFilterProviderResolver;

    public IndexConfigurationStorage(final Directory directoryFacade) {
        this(directoryFacade, ChunkFilterProviderResolverImpl.defaultResolver());
    }

    /**
     * Creates a configuration storage wrapper.
     *
     * @param directoryFacade backing directory
     * @param chunkFilterProviderResolver resolver used to materialize runtime
     *                                    filter registrations while loading
     */
    public IndexConfigurationStorage(final Directory directoryFacade,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        Vldtn.requireNonNull(chunkFilterProviderResolver,
                "chunkFilterProviderResolver");
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
    }

    public EffectiveIndexConfiguration<K, V> load() {
        final PropertyStore props = PropertyStoreImpl
                .fromDirectory(directoryFacade, CONFIGURATION_FILENAME, true);
        SCHEMA.ensure(props);
        final PropertyView propsView = props.snapshot();
        final Class<K> keyClass = toClass(propsView.getString(PROP_KEY_CLASS));
        final Class<V> valueClass = toClass(
                propsView.getString(PROP_VALUE_CLASS));
        final long maxNumberOfKeysInSegmentCache = propsView
                .getLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        final long defaultSegmentWriteCacheKeyLimit = maxNumberOfKeysInSegmentCache > 0
                ? maxNumberOfKeysInSegmentCache / 2
                : IndexConfigurationContract.DEFAULT_SEGMENT_CACHE_KEY_LIMIT
                        / 2;
        final long segmentWriteCacheKeyLimit = getOrDefaultLong(
                propsView, PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT,
                defaultSegmentWriteCacheKeyLimit);
        final long defaultSegmentWriteCacheKeyLimitDuringMaintenance = Math.max(
                segmentWriteCacheKeyLimit * 2,
                segmentWriteCacheKeyLimit + 1);
        final long segmentWriteCacheKeyLimitDuringMaintenance = getOrDefaultLong(propsView,
                PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                defaultSegmentWriteCacheKeyLimitDuringMaintenance);
        final long indexBufferedWriteKeyLimit = getOrDefaultLong(propsView,
                PROP_INDEX_BUFFERED_WRITE_KEY_LIMIT,
                Math.max(segmentWriteCacheKeyLimitDuringMaintenance,
                        segmentWriteCacheKeyLimitDuringMaintenance
                                * Math.max(1L,
                                        propsView.getInt(
                                                PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE))));
        final int maxNumberOfDeltaCacheFiles = getOrDefault(propsView,
                PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES,
                IndexConfigurationContract.DEFAULT_DELTA_CACHE_FILE_LIMIT);
        final int segmentSplitKeyThreshold = getOrDefault(propsView,
                PROP_SEGMENT_SPLIT_KEY_THRESHOLD,
                IndexConfigurationContract.DEFAULT_SEGMENT_SPLIT_KEY_THRESHOLD);
        final int maxNumberOfKeysInSegment = getOrDefault(propsView,
                PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                segmentSplitKeyThreshold);
        final String encodingFilters = propsView
                .getString(PROP_ENCODING_CHUNK_FILTERS);
        final var encodingSpecs = encodingFilters != null
                && !encodingFilters.isBlank()
                        ? ChunkFilterSpecCodec.parse(encodingFilters)
                        : List.<ChunkFilterSpec>of();
        final String decodingFilters = propsView
                .getString(PROP_DECODING_CHUNK_FILTERS);
        final var decodingSpecs = decodingFilters != null
                && !decodingFilters.isBlank()
                        ? ChunkFilterSpecCodec.parse(decodingFilters)
                        : List.<ChunkFilterSpec>of();

        final double falsePositiveProbability = propsView.getDouble(
                PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE) == 0.0d
                        ? BloomFilterBuilder.DEFAULT_PROBABILITY_OF_FALSE_POSITIVE
                        : propsView.getDouble(
                                PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE);
        return new EffectiveIndexConfiguration<>(
                new EffectiveIndexIdentityConfiguration<>(
                        propsView.getString(PROP_INDEX_NAME), keyClass,
                        valueClass,
                        propsView.getString(PROP_KEY_TYPE_DESCRIPTOR),
                        propsView.getString(PROP_VALUE_TYPE_DESCRIPTOR)),
                new EffectiveIndexSegmentConfiguration(
                        maxNumberOfKeysInSegment,
                        propsView.getInt(
                                PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK),
                        (int) maxNumberOfKeysInSegmentCache,
                        propsView.getInt(PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE),
                        maxNumberOfDeltaCacheFiles),
                new EffectiveIndexWritePathConfiguration(
                        (int) segmentWriteCacheKeyLimit,
                        (int) segmentWriteCacheKeyLimitDuringMaintenance,
                        (int) indexBufferedWriteKeyLimit,
                        segmentSplitKeyThreshold),
                new EffectiveIndexBloomFilterConfiguration(
                        propsView.getInt(
                                PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS),
                        propsView.getInt(
                                PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES),
                        falsePositiveProbability),
                new EffectiveIndexMaintenanceConfiguration(
                        getOrDefault(propsView,
                                PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS,
                                IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS),
                        getOrDefault(propsView,
                                PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS,
                                IndexConfigurationContract.DEFAULT_INDEX_MAINTENANCE_THREADS),
                        getOrDefault(propsView,
                                PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS,
                                IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS),
                        getOrDefault(propsView, PROP_INDEX_BUSY_BACKOFF_MILLIS,
                                IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS),
                        getOrDefault(propsView, PROP_INDEX_BUSY_TIMEOUT_MILLIS,
                                IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS),
                        getOrDefaultBoolean(propsView,
                                PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED,
                                IndexConfigurationContract.DEFAULT_BACKGROUND_MAINTENANCE_AUTO_ENABLED)),
                new EffectiveIndexIoConfiguration(propsView
                        .getInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES)),
                new EffectiveIndexLoggingConfiguration(
                        propsView.getBoolean(PROP_CONTEXT_LOGGING_ENABLED)),
                loadWal(propsView),
                EffectiveIndexFilterConfiguration.fromSpecs(encodingSpecs,
                        decodingSpecs, chunkFilterProviderResolver),
                new EffectiveIndexChunkStoreCacheConfiguration(
                        propsView.getInt(
                                PROP_CHUNK_STORE_CACHE_PAGE_LIMIT)));
    }

    /**
     * Persists the provided configuration to the backing property store.
     *
     * @param indexConfiguration configuration to persist
     */
    public void save(EffectiveIndexConfiguration<K, V> indexConfiguration) {
        final var identity = indexConfiguration.identity();
        final var logging = indexConfiguration.logging();
        final var segment = indexConfiguration.segment();
        final var writePath = indexConfiguration.writePath();
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
        writer.setInt(PROP_SEGMENT_SPLIT_KEY_THRESHOLD,
                writePath.segmentSplitKeyThreshold());
        writer.setInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES,
                indexConfiguration.io().diskBufferSizeBytes());

        // Segment properties
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                segment.cacheKeyLimit());
        writer.setLong(PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT,
                writePath.segmentWriteCacheKeyLimit());
        writer.setLong(PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                writePath.segmentWriteCacheKeyLimitDuringMaintenance());
        writer.setLong(PROP_INDEX_BUFFERED_WRITE_KEY_LIMIT,
                writePath.indexBufferedWriteKeyLimit());
        writer.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK,
                segment.chunkKeyLimit());
        writer.setInt(PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES,
                segment.deltaCacheFileLimit());
        final int maintenanceThreads = maintenance.segmentThreads();
        writer.setInt(PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS,
                maintenanceThreads);
        final int indexMaintenanceThreads = maintenance.indexThreads();
        writer.setInt(PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS,
                indexMaintenanceThreads);
        final int registryLifecycleThreads = maintenance.registryLifecycleThreads();
        writer.setInt(PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS,
                registryLifecycleThreads);
        final int busyBackoffMillis = maintenance.busyBackoffMillis();
        writer.setInt(PROP_INDEX_BUSY_BACKOFF_MILLIS, busyBackoffMillis);
        final int busyTimeoutMillis = maintenance.busyTimeoutMillis();
        writer.setInt(PROP_INDEX_BUSY_TIMEOUT_MILLIS, busyTimeoutMillis);
        writer.setBoolean(PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED,
                maintenance.backgroundAutoEnabled());
        // Segment bloom filter properties
        writer.setInt(PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                bloomFilter.hashFunctions());
        writer.setInt(PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES,
                bloomFilter.indexSizeBytes());
        writer.setDouble(PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                bloomFilter.falsePositiveProbability());
        writer.setString(PROP_ENCODING_CHUNK_FILTERS,
                ChunkFilterSpecCodec
                        .serialize(filters.encodingChunkFilterSpecs()));

        writer.setString(PROP_DECODING_CHUNK_FILTERS,
                ChunkFilterSpecCodec
                        .serialize(filters.decodingChunkFilterSpecs()));
        writer.setInt(PROP_CHUNK_STORE_CACHE_PAGE_LIMIT,
                indexConfiguration.chunkStoreCache().pageLimit());
        final EffectiveIndexWalConfiguration wal =
                EffectiveIndexWalConfiguration.orEmpty(
                        indexConfiguration.wal());
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
        SCHEMA.writeMetadata(writer);
        tx.close();
    }

    public boolean exists() {
        return directoryFacade.isFileExists(CONFIGURATION_FILENAME);
    }

    ChunkFilterProviderResolver chunkFilterProviderResolver() {
        return chunkFilterProviderResolver;
    }

    private EffectiveIndexWalConfiguration loadWal(final PropertyView propsView) {
        final boolean walEnabled = getOrDefaultBoolean(propsView,
                PROP_WAL_ENABLED, false);
        if (!walEnabled) {
            return EffectiveIndexWalConfiguration.EMPTY;
        }
        return new EffectiveIndexWalConfiguration(true,
                resolveEnum(propsView, PROP_WAL_DURABILITY_MODE,
                        IndexWalConfiguration.DEFAULT_DURABILITY_MODE,
                        WalDurabilityMode.class),
                getOrDefaultLong(propsView, PROP_WAL_SEGMENT_SIZE_BYTES,
                        IndexWalConfiguration.DEFAULT_SEGMENT_SIZE_BYTES),
                getOrDefault(propsView, PROP_WAL_GROUP_SYNC_DELAY_MILLIS,
                        IndexWalConfiguration.DEFAULT_GROUP_SYNC_DELAY_MILLIS),
                getOrDefault(propsView, PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES,
                        IndexWalConfiguration.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES),
                getOrDefaultLong(propsView,
                        PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                        IndexWalConfiguration.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT),
                resolveEnum(propsView, PROP_WAL_CORRUPTION_POLICY,
                        IndexWalConfiguration.DEFAULT_CORRUPTION_POLICY,
                        WalCorruptionPolicy.class),
                getOrDefaultBoolean(propsView, PROP_WAL_EPOCH_SUPPORT, false));
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
