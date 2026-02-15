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

/**
 * Shared properties schema metadata and migration helpers for index files.
 */
public final class IndexPropertiesSchema {

    public static final String SCHEMA_VERSION_KEY = "schema.version";
    public static final String REQUIRED_KEYS_KEY = "schema.requiredKeys";
    public static final int CURRENT_SCHEMA_VERSION = 1;

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
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE = "maxNumberOfKeysInSegmentWriteCache";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE = "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = "maxNumberOfKeysInSegmentChunk";
        public static final String PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES = "maxNumberOfDeltaCacheFiles";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_CACHE = "maxNumberOfKeysInCache";
        public static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT = "maxNumberOfKeysInSegment";
        public static final String PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = "maxNumberOfSegmentsInCache";
        public static final String PROP_NUMBER_OF_THREADS = "numberOfThreads";
        public static final String PROP_NUMBER_OF_IO_THREADS = "numberOfIoThreads";
        public static final String PROP_SEGMENT_INDEX_MAINTENANCE_THREADS = "segmentIndexMaintenanceThreads";
        public static final String PROP_NUMBER_OF_INDEX_MAINTENANCE_THREADS = "numberOfIndexMaintenanceThreads";
        public static final String PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS = "numberOfRegistryLifecycleThreads";
        public static final String PROP_INDEX_BUSY_BACKOFF_MILLIS = "indexBusyBackoffMillis";
        public static final String PROP_INDEX_BUSY_TIMEOUT_MILLIS = "indexBusyTimeoutMillis";
        public static final String PROP_SEGMENT_MAINTENANCE_AUTO_ENABLED = "segmentMaintenanceAutoEnabled";
        public static final String PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = "bloomFilterNumberOfHashFunctions";
        public static final String PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = "bloomFilterIndexSizeInBytes";
        public static final String PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = "bloomFilterProbabilityOfFalsePositive";
        public static final String PROP_DISK_IO_BUFFER_SIZE_IN_BYTES = "diskIoBufferSizeInBytes";
        public static final String PROP_ENCODING_CHUNK_FILTERS = "encodingChunkFilters";
        public static final String PROP_DECODING_CHUNK_FILTERS = "decodingChunkFilters";

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
        if (schemaVersion > CURRENT_SCHEMA_VERSION) {
            throw new IllegalStateException(String.format(
                    "Unsupported schema version %s for '%s'",
                    schemaVersion, schemaName));
        }
        final Map<String, String> updates = new LinkedHashMap<>();
        final List<String> missing = new ArrayList<>();
        for (final String key : requiredKeys) {
            if (isMissing(view, key)) {
                final DefaultValueProvider provider = defaultProviders.get(key);
                if (provider == null) {
                    missing.add(key);
                } else {
                    final String value = provider.provide(view);
                    if (value == null || (value.isBlank()
                            && !blankAllowedKeys.contains(key))) {
                        missing.add(key);
                    } else {
                        updates.put(key, value);
                    }
                }
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "Missing required properties for '%s': %s",
                    schemaName, String.join(", ", missing)));
        }
        final String requiredKeysValue = serializeRequiredKeys();
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            updates.put(SCHEMA_VERSION_KEY,
                    String.valueOf(CURRENT_SCHEMA_VERSION));
        }
        if (!requiredKeysValue.equals(view.getString(REQUIRED_KEYS_KEY))) {
            updates.put(REQUIRED_KEYS_KEY, requiredKeysValue);
        }
        if (!updates.isEmpty()) {
            try (PropertyTransaction tx = store.beginTransaction()) {
                final PropertyWriter writer = tx.openPropertyWriter();
                for (final Map.Entry<String, String> entry : updates.entrySet()) {
                    writer.setString(entry.getKey(), entry.getValue());
                }
            }
        }
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
        defaults.put(IndexConfigurationKeys.PROP_CONTEXT_LOGGING_ENABLED,
                view -> Boolean.FALSE.toString());
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE));
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE,
                IndexPropertiesSchema::defaultSegmentWriteCache);
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE,
                IndexPropertiesSchema::defaultSegmentWriteCacheDuringMaintenance);
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK));
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_DELTA_CACHE_FILES));
        defaults.put(IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_CACHE,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_CACHE));
        defaults.put(IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT));
        defaults.put(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                view -> String.valueOf(
                        IndexConfigurationContract.MAX_NUMBER_OF_SEGMENTS_IN_CACHE));
        defaults.put(IndexConfigurationKeys.PROP_NUMBER_OF_THREADS,
                view -> String.valueOf(
                        IndexConfigurationContract.NUMBER_OF_THREADS));
        defaults.put(IndexConfigurationKeys.PROP_NUMBER_OF_IO_THREADS,
                view -> String.valueOf(
                        IndexConfigurationContract.NUMBER_OF_IO_THREADS));
        defaults.put(
                IndexConfigurationKeys.PROP_SEGMENT_INDEX_MAINTENANCE_THREADS,
                view -> String.valueOf(
                        IndexConfigurationContract.DEFAULT_SEGMENT_INDEX_MAINTENANCE_THREADS));
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
        defaults.put(
                IndexConfigurationKeys.PROP_SEGMENT_MAINTENANCE_AUTO_ENABLED,
                view -> String.valueOf(
                        IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_AUTO_ENABLED));
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
        defaults.put(IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS,
                view -> "");
        defaults.put(IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS,
                view -> "");

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

    private static String defaultSegmentWriteCache(final PropertyView view) {
        final long segmentCache = resolveSegmentCache(view);
        return String.valueOf(segmentCache / 2);
    }

    private static String defaultSegmentWriteCacheDuringMaintenance(
            final PropertyView view) {
        final long writeCache = resolveSegmentWriteCache(view);
        final long defaultValue = Math.max(writeCache * 2, writeCache + 1);
        return String.valueOf(defaultValue);
    }

    private static long resolveSegmentCache(final PropertyView view) {
        final String value = view.getString(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        if (value != null && !value.isBlank()) {
            return Long.parseLong(value);
        }
        return IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    }

    private static long resolveSegmentWriteCache(final PropertyView view) {
        final String value = view.getString(
                IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE);
        if (value != null && !value.isBlank()) {
            return Long.parseLong(value);
        }
        return resolveSegmentCache(view) / 2;
    }
}
