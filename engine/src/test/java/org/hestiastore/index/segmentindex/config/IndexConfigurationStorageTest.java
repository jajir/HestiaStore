package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyWriter;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.ResolvedIndexConfiguration;
import org.junit.jupiter.api.Test;

class IndexConfigurationStorageTest {

    private static final String LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE = "maxNumberOfKeysInSegmentWriteCache";
    private static final String LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE = "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance";
    private static final String LEGACY_NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS = "numberOfStableSegmentMaintenanceThreads";
    private static final String LEGACY_SEGMENT_MAINTENANCE_AUTO_ENABLED = "segmentMaintenanceAutoEnabled";
    private static final String LEGACY_SEGMENT_INDEX_MAINTENANCE_THREADS = "segmentIndexMaintenanceThreads";
    private static final String TEST_PROVIDER_ID = "test-custom";
    private static final String TEST_PARAM_KEY_REF = "keyRef";

    @Test
    void existsReflectsConfigurationPresence() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);

        assertFalse(storage.exists());

        storage.save(buildConf());

        assertTrue(storage.exists());
        final IndexConfiguration<String, String> loaded = storage.load();
        assertEquals("index-config-storage-test", loaded.identity().name());
        assertEquals(2, loaded.writePath().segmentWriteCacheKeyLimit());
        assertEquals(2, loaded.runtimeTuning().legacyImmutableRunLimit());
        assertEquals(3, loaded.writePath().segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(9, loaded.writePath().indexBufferedWriteKeyLimit());
        assertEquals(11, loaded.segment().maxKeys());
        assertEquals(10, loaded.writePath().segmentSplitKeyThreshold());
        assertEquals(7, loaded.maintenance().segmentThreads());
        assertFalse(loaded.maintenance().backgroundAutoEnabled());
    }

    @Test
    void groupedConfigurationRoundTripsThroughStorage() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<String, String> configuration =
                IndexConfiguration.<String, String>builder()
                        .identity(identity -> identity.name("grouped-storage")
                                .keyClass(String.class)
                                .valueClass(String.class)
                                .keyTypeDescriptor(typeDescriptor)
                                .valueTypeDescriptor(typeDescriptor))
                        .segment(segment -> segment.maxKeys(100)
                                .chunkKeyLimit(10).cacheKeyLimit(20)
                                .cachedSegmentLimit(4)
                                .deltaCacheFileLimit(3))
                        .writePath(writePath -> writePath
                                .segmentWriteCacheKeyLimit(7)
                                .maintenanceWriteCacheKeyLimit(9)
                                .indexBufferedWriteKeyLimit(40)
                                .segmentSplitKeyThreshold(80))
                        .bloomFilter(bloom -> bloom.hashFunctions(2)
                                .indexSizeBytes(1024)
                                .falsePositiveProbability(0.05D))
                        .maintenance(maintenance -> maintenance
                                .segmentThreads(2).indexThreads(3)
                                .registryLifecycleThreads(4)
                                .busyBackoffMillis(5)
                                .busyTimeoutMillis(6)
                                .backgroundAutoEnabled(false))
                        .io(io -> io.diskBufferSizeBytes(2048))
                        .logging(logging -> logging.contextEnabled(false))
                        .filters(filters -> filters
                                .encodingFilterSpecs(
                                        List.of(ChunkFilterSpecs.doNothing()))
                                .decodingFilterSpecs(
                                        List.of(ChunkFilterSpecs.doNothing())))
                        .build();

        storage.save(configuration);
        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals("grouped-storage", loaded.identity().name());
        assertEquals(Integer.valueOf(100), loaded.segment().maxKeys());
        assertEquals(Integer.valueOf(7),
                loaded.writePath().segmentWriteCacheKeyLimit());
        assertEquals(Integer.valueOf(1024),
                loaded.bloomFilter().indexSizeBytes());
        assertFalse(loaded.maintenance().backgroundAutoEnabled());
        assertEquals(Integer.valueOf(2048), loaded.io().diskBufferSizeBytes());
        assertFalse(loaded.logging().contextEnabled());
        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                loaded.filters().encodingChunkFilterSpecs());
    }

    @Test
    void loadMigratesLegacyPartitionSettingsIntoNewNames() {
        final MemDirectory directory = new MemDirectory();
        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "legacy-config");
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                    4);
            writer.setLong(LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE, 5L);
            writer.setLong(
                    LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE,
                    9L);
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                    30);
        }

        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);
        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals(5, loaded.writePath().segmentWriteCacheKeyLimit());
        assertEquals(9, loaded.writePath().segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(36, loaded.writePath().indexBufferedWriteKeyLimit());
        assertEquals(30, loaded.segment().maxKeys());
        assertEquals(30, loaded.writePath().segmentSplitKeyThreshold());
    }

    @Test
    void loadMigratesLegacyBackgroundMaintenanceFlagIntoNewName() {
        final MemDirectory directory = new MemDirectory();
        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "legacy-background-maintenance-config");
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                    4);
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                    30);
            writer.setBoolean(LEGACY_SEGMENT_MAINTENANCE_AUTO_ENABLED, false);
        }

        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);
        final IndexConfiguration<String, String> loaded = storage.load();

        assertFalse(loaded.maintenance().backgroundAutoEnabled());
    }

    @Test
    void loadMigratesPreviousPublicSegmentMaintenanceKeyIntoNewName() {
        final MemDirectory directory = new MemDirectory();
        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "previous-public-segment-maintenance-config");
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                    4);
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                    30);
            writer.setInt(
                    LEGACY_NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS, 6);
        }

        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);
        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals(6, loaded.maintenance().segmentThreads());
    }

    @Test
    void loadMigratesLegacySegmentIndexMaintenanceThreadsIntoNewName() {
        final MemDirectory directory = new MemDirectory();
        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    typeDescriptor.getClass().getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "legacy-segment-index-maintenance-config");
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                    4);
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                    30);
            writer.setInt(LEGACY_SEGMENT_INDEX_MAINTENANCE_THREADS, 6);
        }

        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);
        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals(6, loaded.maintenance().segmentThreads());
    }

    @Test
    void savePersistsNewMaintenanceKeysAndRemovesLegacyAliases() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);

        storage.save(buildConf());

        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                true);
        final var view = store.snapshot();

        assertEquals(7, view.getInt(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_NUMBER_OF_SEGMENT_MAINTENANCE_THREADS));
        assertFalse(view.getBoolean(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED));
        assertNull(view.getString(LEGACY_SEGMENT_MAINTENANCE_AUTO_ENABLED));
        assertNull(view.getString(
                LEGACY_NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS));
        assertNull(view.getString(LEGACY_SEGMENT_INDEX_MAINTENANCE_THREADS));
        assertNull(view.getString(LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE));
        assertNull(view.getString(
                LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE));
    }

    @Test
    void loadSupportsLegacyChunkFilterClassNames() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);
        storage.save(buildConf());

        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                true);
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS,
                    ChunkFilterMagicNumberWriting.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS,
                    ChunkFilterMagicNumberValidation.class.getName());
        }

        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals(List.of(ChunkFilterSpecs.magicNumber()),
                loaded.filters().encodingChunkFilterSpecs());
        assertEquals(List.of(ChunkFilterSpecs.magicNumber()),
                loaded.filters().decodingChunkFilterSpecs());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                loaded.resolveRuntimeConfiguration().getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberValidation.class,
                loaded.resolveRuntimeConfiguration().getDecodingChunkFilters().get(0).getClass());
    }

    @Test
    void loadKeepsUnknownLegacyChunkFilterClassNamesOnJavaClassProvider() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);
        storage.save(buildConf());

        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                true);
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS,
                    LegacyCustomChunkFilter.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS,
                    LegacyCustomChunkFilter.class.getName());
        }

        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals(List.of(
                ChunkFilterSpecs.javaClass(LegacyCustomChunkFilter.class.getName())),
                loaded.filters().encodingChunkFilterSpecs());
        assertEquals(List.of(
                ChunkFilterSpecs.javaClass(LegacyCustomChunkFilter.class.getName())),
                loaded.filters().decodingChunkFilterSpecs());
        assertEquals(LegacyCustomChunkFilter.class,
                loaded.resolveRuntimeConfiguration().getEncodingChunkFilters().get(0).getClass());
        assertEquals(LegacyCustomChunkFilter.class,
                loaded.resolveRuntimeConfiguration().getDecodingChunkFilters().get(0).getClass());
    }

    @Test
    void saveAndLoadRoundTripWithCustomProviderSpec() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver registry = ChunkFilterProviderResolverImpl
                .builder().withDefaultProviders()
                .withProvider(new TestChunkFilterProvider())
                .build();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory, registry);
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider(TEST_PROVIDER_ID)
                .withParameter(TEST_PARAM_KEY_REF, "orders-main");
        final IndexConfiguration<String, String> config = IndexConfiguration
                .<String, String>builder()//
                .identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity.name("custom-provider-test"))//
                .segment(segment -> segment.cacheKeyLimit(4))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(2))//
                .writePath(writePath -> writePath.legacyImmutableRunLimit(2))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(3))//
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(11))//
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(10))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .maintenance(maintenance -> maintenance.segmentThreads(7))//
                .logging(logging -> logging.contextEnabled(false))//
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))//
                .filters(filters -> filters
                        .addEncodingFilter(spec)
                        .addDecodingFilter(spec))//
                .build();

        storage.save(config);

        final IndexConfiguration<String, String> loaded = storage.load();
        final ResolvedIndexConfiguration<String, String> runtimeConfiguration = loaded
                .resolveRuntimeConfiguration(registry);

        assertEquals(List.of(spec), loaded.filters().encodingChunkFilterSpecs());
        assertEquals(List.of(spec), loaded.filters().decodingChunkFilterSpecs());

        final TestEncodingChunkFilter encodingFilter = assertInstanceOf(
                TestEncodingChunkFilter.class,
                runtimeConfiguration.getEncodingChunkFilters().get(0));
        final TestDecodingChunkFilter decodingFilter = assertInstanceOf(
                TestDecodingChunkFilter.class,
                runtimeConfiguration.getDecodingChunkFilters().get(0));

        assertEquals("orders-main", encodingFilter.getKeyRef());
        assertEquals("orders-main", decodingFilter.getKeyRef());
    }

    @Test
    void saveAndLoadRoundTripPreservesChunkFilterSpecsByValue() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver registry = ChunkFilterProviderResolverImpl
                .builder().withDefaultProviders()
                .withProvider(new TestChunkFilterProvider())
                .build();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory, registry);
        final ChunkFilterSpec customSpec = ChunkFilterSpec
                .ofProvider(TEST_PROVIDER_ID)
                .withParameter(TEST_PARAM_KEY_REF, "orders-archive");
        final IndexConfiguration<String, String> config = IndexConfiguration
                .<String, String>builder()//
                .identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity.name("spec-roundtrip-test"))//
                .segment(segment -> segment.cacheKeyLimit(4))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(2))//
                .writePath(writePath -> writePath.legacyImmutableRunLimit(2))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(3))//
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(11))//
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(10))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .maintenance(maintenance -> maintenance.segmentThreads(7))//
                .logging(logging -> logging.contextEnabled(false))//
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))//
                .filters(filters -> filters
                        .addEncodingFilter(new ChunkFilterDoNothing())
                        .addEncodingFilter(customSpec)
                        .addDecodingFilter(customSpec)
                        .addDecodingFilter(new ChunkFilterDoNothing()))//
                .build();

        storage.save(config);

        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals(config.filters().encodingChunkFilterSpecs(),
                loaded.filters().encodingChunkFilterSpecs());
        assertEquals(config.filters().decodingChunkFilterSpecs(),
                loaded.filters().decodingChunkFilterSpecs());
        assertNotSame(config.filters().encodingChunkFilterSpecs(),
                loaded.filters().encodingChunkFilterSpecs());
        assertNotSame(config.filters().decodingChunkFilterSpecs(),
                loaded.filters().decodingChunkFilterSpecs());
        assertNotSame(config.filters().encodingChunkFilterSpecs().get(1),
                loaded.filters().encodingChunkFilterSpecs().get(1));
        assertNotSame(config.filters().decodingChunkFilterSpecs().get(0),
                loaded.filters().decodingChunkFilterSpecs().get(0));
    }

    private IndexConfiguration<String, String> buildConf() {
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        return IndexConfiguration.<String, String>builder()//
                .identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(typeDescriptor))//
                .identity(identity -> identity.valueTypeDescriptor(typeDescriptor))//
                .identity(identity -> identity.name("index-config-storage-test"))//
                .segment(segment -> segment.cacheKeyLimit(4))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(2))//
                .writePath(writePath -> writePath.legacyImmutableRunLimit(2))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(3))//
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(11))//
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(10))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .maintenance(maintenance -> maintenance.segmentThreads(7))//
                .logging(logging -> logging.contextEnabled(false))//
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))//
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))//
                .build();
    }

    private static final class TestChunkFilterProvider
            implements ChunkFilterProvider {

        @Override
        public String getProviderId() {
            return TEST_PROVIDER_ID;
        }

        @Override
        public java.util.function.Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            final String keyRef = spec.getRequiredParameter(TEST_PARAM_KEY_REF);
            return () -> new TestEncodingChunkFilter(keyRef);
        }

        @Override
        public java.util.function.Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            final String keyRef = spec.getRequiredParameter(TEST_PARAM_KEY_REF);
            return () -> new TestDecodingChunkFilter(keyRef);
        }
    }

    private abstract static class AbstractTestChunkFilter
            implements ChunkFilter {

        private final String keyRef;

        protected AbstractTestChunkFilter(final String keyRef) {
            this.keyRef = keyRef;
        }

        String getKeyRef() {
            return keyRef;
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }

    private static final class TestEncodingChunkFilter
            extends AbstractTestChunkFilter {

        private TestEncodingChunkFilter(final String keyRef) {
            super(keyRef);
        }
    }

    private static final class TestDecodingChunkFilter
            extends AbstractTestChunkFilter {

        private TestDecodingChunkFilter(final String keyRef) {
            super(keyRef);
        }
    }

}
