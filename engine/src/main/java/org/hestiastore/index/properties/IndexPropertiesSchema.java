package org.hestiastore.index.properties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterBuilder;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.Wal;

/**
 * Shared properties schema metadata and migration helpers for index files.
 */
public final class IndexPropertiesSchema {

    public static final String SCHEMA_VERSION_KEY = "schema.version";
    public static final String REQUIRED_KEYS_KEY = "schema.requiredKeys";
    public static final int CURRENT_SCHEMA_VERSION = 1;
    private static final String LEGACY_PROP_NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS = "numberOfStableSegmentMaintenanceThreads";
    private static final String LEGACY_PROP_SEGMENT_MAINTENANCE_AUTO_ENABLED = "segmentMaintenanceAutoEnabled";
    private static final String LEGACY_PROP_SEGMENT_INDEX_MAINTENANCE_THREADS = "segmentIndexMaintenanceThreads";
    private static final String LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE = "maxNumberOfKeysInSegmentWriteCache";
    private static final String LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE = "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance";

    private static final String REQUIRED_KEYS_SEPARATOR = ",";

    /**
     * Keys used in per-segment properties files.
     */
    public static final class SegmentKeys {
        public static final String NUMBER_OF_KEYS_IN_DELTA_CACHE = "numberOfKeysInDeltaCache";
        public static final String NUMBER_OF_KEYS_IN_MAIN_INDEX = "numberOfKeysInMainIndex";
        public static final String NUMBER_OF_KEYS_IN_SCARCE_INDEX = "numberOfKeysInScarceIndex";
        public static final String NUMBER_OF_SEGMENT_CACHE_DELTA_FILES = "numberOfSegmentDeltaFiles";
        public static final String SEGMENT_VERSION = "segmentVersion";

        private SegmentKeys() {
        }
    }

    /**
     * Keys used in index configuration properties files.
     */
    public static final class IndexConfigurationKeys {
        public static final String PROP_KEY_CLASS = "keyClass";
        public static final String PROP_VALUE_CLASS = "valueClass";
        public static final String PROP_KEY_TYPE_DESCRIPTOR = "keyTypeDescriptor";
        public static final String PROP_VALUE_TYPE_DESCRIPTOR = "valueTypeDescriptor";
        public static final String PROP_INDEX_NAME = "indexName";
        public static final String PROP_CONTEXT_LOGGING_ENABLED = "contextLoggingEnabled";

        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = "maxNumberOfKeysInSegmentCache";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION = "maxNumberOfKeysInActivePartition";
        public static final String PROP_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION = "maxNumberOfImmutableRunsPerPartition";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER = "maxNumberOfKeysInPartitionBuffer";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER = "maxNumberOfKeysInIndexBuffer";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = "maxNumberOfKeysInSegmentChunk";
        public static final String PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES = "maxNumberOfDeltaCacheFiles";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT = "maxNumberOfKeysInSegment";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT = "maxNumberOfKeysInPartitionBeforeSplit";
        public static final String PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = "maxNumberOfSegmentsInCache";
        public static final String PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS = "numberOfSegmentMaintenanceThreads";
        public static final String PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS = "numberOfIndexMaintenanceThreads";
        public static final String PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS = "numberOfRegistryLifecycleThreads";
        public static final String PROP_INDEX_BUSY_BACKOFF_MILLIS = "indexBusyBackoffMillis";
        public static final String PROP_INDEX_BUSY_TIMEOUT_MILLIS = "indexBusyTimeoutMillis";
        public static final String PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED = "backgroundMaintenanceAutoEnabled";
        public static final String PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = "bloomFilterNumberOfHashFunctions";
        public static final String PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = "bloomFilterIndexSizeInBytes";
        public static final String PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = "bloomFilterProbabilityOfFalsePositive";
        public static final String PROP_DISK_IO_BUFFER_SIZE_IN_BYTES = "diskIoBufferSizeInBytes";
        public static final String PROP_ENCODING_CHUNK_FILTERS = "encodingChunkFilters";
        public static final String PROP_DECODING_CHUNK_FILTERS = "decodingChunkFilters";
        public static final String PROP_WAL_ENABLED = "wal.enabled";
        public static final String PROP_WAL_DURABILITY_MODE = "wal.durabilityMode";
        public static final String PROP_WAL_SEGMENT_SIZE_BYTES = "wal.segmentSizeBytes";
        public static final String PROP_WAL_GROUP_SYNC_DELAY_MILLIS = "wal.groupSyncDelayMillis";
        public static final String PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES = "wal.groupSyncMaxBatchBytes";
        public static final String PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT = "wal.maxBytesBeforeForcedCheckpoint";
        public static final String PROP_WAL_CORRUPTION_POLICY = "wal.corruptionPolicy";
        public static final String PROP_WAL_EPOCH_SUPPORT = "wal.epochSupport";

        public static final String CONFIGURATION_FILENAME = "manifest.txt";

        private IndexConfigurationKeys() {
        }
    }

    public static final IndexPropertiesSchema SEGMENT_SCHEMA = buildSegmentSchema();
    public static final IndexPropertiesSchema INDEX_CONFIGURATION_SCHEMA = buildIndexConfigurationSchema();

    @FunctionalInterface
    private interface DefaultValueProvider {
        String provide(PropertyView view);
    }

    private final String schemaName;
    private final List<String> requiredKeys;
    private final Map<String, DefaultValueProvider> defaultProviders;
    private final Set<String> blankAllowedKeys;

    private IndexPropertiesSchema(final String schemaName,
            final Collection<String> requiredKeys,
            final Map<String, DefaultValueProvider> defaultProviders) {
        this(schemaName, requiredKeys, defaultProviders, Set.of());
    }

    private IndexPropertiesSchema(final String schemaName,
            final Collection<String> requiredKeys,
            final Map<String, DefaultValueProvider> defaultProviders,
            final Collection<String> blankAllowedKeys) {
        this.schemaName = Vldtn.requireNonNull(schemaName, "schemaName");
        this.requiredKeys = deduplicate(requiredKeys);
        this.defaultProviders = Map.copyOf(defaultProviders);
        this.blankAllowedKeys = Set.copyOf(
                Vldtn.requireNonNull(blankAllowedKeys, "blankAllowedKeys"));
        ensureDefaultsAreRequired();
    }

    /**
     * Ensures schema metadata and required keys are present, applying defaults
     * when allowed.
     *
     * @param store property store to update when needed
     */
    public void ensure(final PropertyStore store) {
        Vldtn.requireNonNull(store, "store");
        final PropertyView view = store.snapshot();
        final int schemaVersion = view.getInt(SCHEMA_VERSION_KEY);
        ensureSchemaVersionSupported(schemaVersion);
        final Map<String, String> updates = new LinkedHashMap<>();
        final List<String> missing = new ArrayList<>();
        collectRequiredKeyUpdates(view, updates, missing);
        ensureNoMissingRequired(missing);
        appendSchemaMetadataUpdates(view, schemaVersion, updates);
        persistUpdates(store, updates);
    }

    /**
     * Writes schema metadata into an existing transaction.
     *
     * @param writer property writer
     */
    public void writeMetadata(final PropertyWriter writer) {
        Vldtn.requireNonNull(writer, "writer");
        writer.setInt(SCHEMA_VERSION_KEY, CURRENT_SCHEMA_VERSION);
        writer.setString(REQUIRED_KEYS_KEY, serializeRequiredKeys());
    }

    List<String> getRequiredKeys() {
        return requiredKeys;
    }

    private boolean isMissing(final PropertyView view, final String key) {
        final String value = view.getString(key);
        if (value == null) {
            return true;
        }
        return value.isBlank() && !blankAllowedKeys.contains(key);
    }

    private void ensureSchemaVersionSupported(final int schemaVersion) {
        if (schemaVersion > CURRENT_SCHEMA_VERSION) {
            throw new IllegalStateException(String.format(
                    "Unsupported schema version %s for '%s'",
                    schemaVersion, schemaName));
        }
    }

    private void collectRequiredKeyUpdates(final PropertyView view,
            final Map<String, String> updates, final List<String> missing) {
        for (final String key : requiredKeys) {
            if (!isMissing(view, key)) {
                continue;
            }
            final String defaultValue = resolveDefaultValue(view, key);
            if (defaultValue == null) {
                missing.add(key);
            } else {
                updates.put(key, defaultValue);
            }
        }
    }

    private String resolveDefaultValue(final PropertyView view, final String key) {
        final DefaultValueProvider provider = defaultProviders.get(key);
        if (provider == null) {
            return null;
        }
        final String value = provider.provide(view);
        final boolean blankValueNotAllowed = value != null && value.isBlank()
                && !blankAllowedKeys.contains(key);
        if (value == null || blankValueNotAllowed) {
            return null;
        }
        return value;
    }

    private void ensureNoMissingRequired(final List<String> missing) {
        if (!missing.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "Missing required properties for '%s': %s",
                    schemaName, String.join(", ", missing)));
        }
    }

    private void appendSchemaMetadataUpdates(final PropertyView view,
            final int schemaVersion, final Map<String, String> updates) {
        final String requiredKeysValue = serializeRequiredKeys();
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            updates.put(SCHEMA_VERSION_KEY,
                    String.valueOf(CURRENT_SCHEMA_VERSION));
        }
        if (!requiredKeysValue.equals(view.getString(REQUIRED_KEYS_KEY))) {
            updates.put(REQUIRED_KEYS_KEY, requiredKeysValue);
        }
    }

    private static void persistUpdates(final PropertyStore store,
            final Map<String, String> updates) {
        if (updates.isEmpty()) {
            return;
        }
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            for (final Map.Entry<String, String> entry : updates.entrySet()) {
                writer.setString(entry.getKey(), entry.getValue());
            }
        }
    }

    private String serializeRequiredKeys() {
        return String.join(REQUIRED_KEYS_SEPARATOR, requiredKeys);
    }

    private void ensureDefaultsAreRequired() {
        for (final String key : defaultProviders.keySet()) {
            if (!requiredKeys.contains(key)) {
                throw new IllegalArgumentException(String.format(
                        "Default value configured for non-required key '%s'",
                        key));
            }
        }
    }

    private static List<String> deduplicate(
            final Collection<String> requiredKeys) {
        Vldtn.requireNonNull(requiredKeys, "requiredKeys");
        final Set<String> deduped = new LinkedHashSet<>();
        for (final String key : requiredKeys) {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException(
                        "Required property key must be non-empty");
            }
            deduped.add(key);
        }
        return List.copyOf(deduped);
    }

    private static IndexPropertiesSchema buildSegmentSchema() {
        final Map<String, DefaultValueProvider> defaults = new LinkedHashMap<>();
        defaults.put(SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE,
                view -> "0");
        defaults.put(SegmentKeys.NUMBER_OF_KEYS_IN_MAIN_INDEX, view -> "0");
        defaults.put(SegmentKeys.NUMBER_OF_KEYS_IN_SCARCE_INDEX,
                view -> "0");
        defaults.put(SegmentKeys.NUMBER_OF_SEGMENT_CACHE_DELTA_FILES,
                view -> "0");
        defaults.put(SegmentKeys.SEGMENT_VERSION, view -> "0");
        return new IndexPropertiesSchema("segment-properties",
                defaults.keySet(), defaults);
    }

    private static IndexPropertiesSchema buildIndexConfigurationSchema() {
        final Map<String, DefaultValueProvider> defaults = new LinkedHashMap<>();
        addCoreDefaults(defaults);
        addSegmentDefaults(defaults);
        addThreadingDefaults(defaults);
        addBloomAndIoDefaults(defaults);
        addChunkFilterDefaults(defaults);
        addWalDefaults(defaults);

        final Set<String> requiredKeys = new LinkedHashSet<>();
        requiredKeys.add(IndexConfigurationKeys.PROP_KEY_CLASS);
        requiredKeys.add(IndexConfigurationKeys.PROP_VALUE_CLASS);
        requiredKeys.add(IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR);
        requiredKeys.add(IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR);
        requiredKeys.add(IndexConfigurationKeys.PROP_INDEX_NAME);
        requiredKeys.addAll(defaults.keySet());
        final Set<String> blankAllowed = Set.of(
                IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS,
                IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS);
        return new IndexPropertiesSchema("index-configuration", requiredKeys,
                defaults, blankAllowed);
    }

    private static void addCoreDefaults(
            final Map<String, DefaultValueProvider> defaults) {
        defaults.put(IndexConfigurationKeys.PROP_CONTEXT_LOGGING_ENABLED,
                view -> Boolean.FALSE.toString());
        defaults.put(
                IndexConfigurationKeys.PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED,
                IndexPropertiesSchema::defaultBackgroundMaintenanceAutoEnabled);
    }

    private static void addSegmentDefaults(
            final Map<String, DefaultValueProvider> defaults) {
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE));
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                IndexPropertiesSchema::defaultActivePartition);
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
                view -> String.valueOf(IndexConfigurationContract.DEFAULT_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION));
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                IndexPropertiesSchema::defaultPartitionBuffer);
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                IndexPropertiesSchema::defaultIndexBuffer);
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK));
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_DELTA_CACHE_FILES));
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                IndexPropertiesSchema::defaultPartitionBeforeSplit);
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_SEGMENTS_IN_CACHE));
    }

    private static void addThreadingDefaults(
            final Map<String, DefaultValueProvider> defaults) {
        defaults.put(
                IndexConfigurationKeys.PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS,
                IndexPropertiesSchema::defaultSegmentMaintenanceThreads);
        defaults.put(
                IndexConfigurationKeys.PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS,
                view -> String.valueOf(
                        IndexConfigurationContract.DEFAULT_INDEX_MAINTENANCE_THREADS));
        defaults.put(
                IndexConfigurationKeys.PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS,
                view -> String.valueOf(
                        IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS));
        defaults.put(IndexConfigurationKeys.PROP_INDEX_BUSY_BACKOFF_MILLIS,
                view -> String.valueOf(
                        IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS));
        defaults.put(IndexConfigurationKeys.PROP_INDEX_BUSY_TIMEOUT_MILLIS,
                view -> String.valueOf(
                        IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS));
    }

    private static void addBloomAndIoDefaults(
            final Map<String, DefaultValueProvider> defaults) {
        defaults.put(
                IndexConfigurationKeys.PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                view -> String.valueOf(
                        IndexConfigurationContract.BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS));
        defaults.put(
                IndexConfigurationKeys.PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES,
                view -> String.valueOf(
                        IndexConfigurationContract.BLOOM_FILTER_INDEX_SIZE_IN_BYTES));
        defaults.put(
                IndexConfigurationKeys.PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                view -> String.valueOf(
                        BloomFilterBuilder.DEFAULT_PROBABILITY_OF_FALSE_POSITIVE));
        defaults.put(
                IndexConfigurationKeys.PROP_DISK_IO_BUFFER_SIZE_IN_BYTES,
                view -> String.valueOf(
                        IndexConfigurationContract.DISK_IO_BUFFER_SIZE_IN_BYTES));
    }

    private static void addChunkFilterDefaults(
            final Map<String, DefaultValueProvider> defaults) {
        defaults.put(IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS,
                view -> "");
        defaults.put(IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS,
                view -> "");
    }

    private static void addWalDefaults(
            final Map<String, DefaultValueProvider> defaults) {
        defaults.put(IndexConfigurationKeys.PROP_WAL_ENABLED,
                view -> Boolean.FALSE.toString());
        defaults.put(IndexConfigurationKeys.PROP_WAL_DURABILITY_MODE,
                view -> Wal.DEFAULT_DURABILITY_MODE.name());
        defaults.put(IndexConfigurationKeys.PROP_WAL_SEGMENT_SIZE_BYTES,
                view -> String.valueOf(Wal.DEFAULT_SEGMENT_SIZE_BYTES));
        defaults.put(IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_DELAY_MILLIS,
                view -> String.valueOf(Wal.DEFAULT_GROUP_SYNC_DELAY_MILLIS));
        defaults.put(IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES,
                view -> String
                        .valueOf(Wal.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES));
        defaults.put(
                IndexConfigurationKeys.PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                view -> String.valueOf(
                        Wal.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT));
        defaults.put(IndexConfigurationKeys.PROP_WAL_CORRUPTION_POLICY,
                view -> Wal.DEFAULT_CORRUPTION_POLICY.name());
        defaults.put(IndexConfigurationKeys.PROP_WAL_EPOCH_SUPPORT,
                view -> Boolean.FALSE.toString());
    }

    private static String defaultBackgroundMaintenanceAutoEnabled(
            final PropertyView view) {
        final String legacy = view.getString(
                LEGACY_PROP_SEGMENT_MAINTENANCE_AUTO_ENABLED);
        if (legacy != null && !legacy.isBlank()) {
            return legacy;
        }
        return String.valueOf(
                IndexConfigurationContract.DEFAULT_BACKGROUND_MAINTENANCE_AUTO_ENABLED);
    }

    private static String defaultSegmentMaintenanceThreads(
            final PropertyView view) {
        final String previousPublicKey = view.getString(
                LEGACY_PROP_NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS);
        if (previousPublicKey != null && !previousPublicKey.isBlank()) {
            return previousPublicKey;
        }
        final String legacy = view.getString(
                LEGACY_PROP_SEGMENT_INDEX_MAINTENANCE_THREADS);
        if (legacy != null && !legacy.isBlank()) {
            return legacy;
        }
        return String.valueOf(
                IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS);
    }

    private static String defaultActivePartition(final PropertyView view) {
        final String legacy = view.getString(
                LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE);
        if (legacy != null && !legacy.isBlank()) {
            return legacy;
        }
        final long segmentCache = resolveSegmentCache(view);
        return String.valueOf(segmentCache / 2);
    }

    private static String defaultPartitionBuffer(
            final PropertyView view) {
        final String legacy = view.getString(
                LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE);
        if (legacy != null && !legacy.isBlank()) {
            return legacy;
        }
        final long activePartitionKeyLimit = resolveActivePartition(view);
        final long defaultValue = Math.max(activePartitionKeyLimit * 2,
                activePartitionKeyLimit + 1);
        return String.valueOf(defaultValue);
    }

    private static String defaultIndexBuffer(final PropertyView view) {
        final long partitionBuffer = resolvePartitionBuffer(view);
        final String segments = view.getString(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE);
        final long segmentCount = segments == null || segments.isBlank()
                ? IndexConfigurationContract.MAX_NUMBER_OF_SEGMENTS_IN_CACHE
                : Long.parseLong(segments);
        return String.valueOf(Math.max(partitionBuffer,
                partitionBuffer * Math.max(1L, segmentCount)));
    }

    private static String defaultPartitionBeforeSplit(final PropertyView view) {
        final String segmentKeyLimit = view.getString(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT);
        if (segmentKeyLimit != null && !segmentKeyLimit.isBlank()) {
            return segmentKeyLimit;
        }
        return String.valueOf(
                IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT);
    }

    private static long resolveSegmentCache(final PropertyView view) {
        final String value = view.getString(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        if (value != null && !value.isBlank()) {
            return Long.parseLong(value);
        }
        return IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    }

    private static long resolveActivePartition(final PropertyView view) {
        final String value = view.getString(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION);
        if (value != null && !value.isBlank()) {
            return Long.parseLong(value);
        }
        final String legacy = view.getString(
                LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE);
        if (legacy != null && !legacy.isBlank()) {
            return Long.parseLong(legacy);
        }
        return resolveSegmentCache(view) / 2;
    }

    private static long resolvePartitionBuffer(final PropertyView view) {
        final String value = view.getString(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER);
        if (value != null && !value.isBlank()) {
            return Long.parseLong(value);
        }
        final String legacy = view.getString(
                LEGACY_PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE);
        if (legacy != null && !legacy.isBlank()) {
            return Long.parseLong(legacy);
        }
        final long activePartitionKeyLimit = resolveActivePartition(view);
        return Math.max(activePartitionKeyLimit * 2,
                activePartitionKeyLimit + 1);
    }
}
