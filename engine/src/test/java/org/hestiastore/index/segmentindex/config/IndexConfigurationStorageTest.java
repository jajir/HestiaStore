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
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
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
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.junit.jupiter.api.Test;

class IndexConfigurationStorageTest {

    private static final String LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE = "maxNumberOfKeysInSegmentWriteCache";
    private static final String LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE = "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance";
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
        assertEquals("index-config-storage-test", loaded.getIndexName());
        assertEquals(2, loaded.getMaxNumberOfKeysInActivePartition());
        assertEquals(2, loaded.getMaxNumberOfImmutableRunsPerPartition());
        assertEquals(3, loaded.getMaxNumberOfKeysInPartitionBuffer());
        assertEquals(9, loaded.getMaxNumberOfKeysInIndexBuffer());
        assertEquals(11, loaded.getMaxNumberOfKeysInSegment());
        assertEquals(10, loaded.getMaxNumberOfKeysInPartitionBeforeSplit());
        assertEquals(7, loaded.getNumberOfStableSegmentMaintenanceThreads());
        assertFalse(loaded.isBackgroundMaintenanceAutoEnabled());
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

        assertEquals(5, loaded.getMaxNumberOfKeysInActivePartition());
        assertEquals(9, loaded.getMaxNumberOfKeysInPartitionBuffer());
        assertEquals(36, loaded.getMaxNumberOfKeysInIndexBuffer());
        assertEquals(30, loaded.getMaxNumberOfKeysInSegment());
        assertEquals(30, loaded.getMaxNumberOfKeysInPartitionBeforeSplit());
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

        assertFalse(loaded.isBackgroundMaintenanceAutoEnabled());
    }

    @Test
    void loadMigratesLegacyStableSegmentMaintenanceThreadsIntoNewName() {
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
                    "legacy-stable-maintenance-config");
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

        assertEquals(6, loaded.getNumberOfStableSegmentMaintenanceThreads());
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
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS));
        assertFalse(view.getBoolean(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_BACKGROUND_MAINTENANCE_AUTO_ENABLED));
        assertNull(view.getString(LEGACY_SEGMENT_MAINTENANCE_AUTO_ENABLED));
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
                loaded.getEncodingChunkFilterSpecs());
        assertEquals(List.of(ChunkFilterSpecs.magicNumber()),
                loaded.getDecodingChunkFilterSpecs());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                loaded.getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberValidation.class,
                loaded.getDecodingChunkFilters().get(0).getClass());
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
                loaded.getEncodingChunkFilterSpecs());
        assertEquals(List.of(
                ChunkFilterSpecs.javaClass(LegacyCustomChunkFilter.class.getName())),
                loaded.getDecodingChunkFilterSpecs());
        assertEquals(LegacyCustomChunkFilter.class,
                loaded.getEncodingChunkFilters().get(0).getClass());
        assertEquals(LegacyCustomChunkFilter.class,
                loaded.getDecodingChunkFilters().get(0).getClass());
    }

    @Test
    void saveAndLoadRoundTripWithCustomProviderSpec() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .builder().withDefaultProviders()
                .withProvider(new TestChunkFilterProvider())
                .build();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory, registry);
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider(TEST_PROVIDER_ID)
                .withParameter(TEST_PARAM_KEY_REF, "orders-main");
        final IndexConfiguration<String, String> config = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorShortString())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("custom-provider-test")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInActivePartition(2)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(3)//
                .withMaxNumberOfKeysInIndexBuffer(9)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(11)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfStableSegmentMaintenanceThreads(7)//
                .withContextLoggingEnabled(false)//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .addEncodingFilter(() -> new TestEncodingChunkFilter("orders-main"),
                        spec)//
                .addDecodingFilter(() -> new TestDecodingChunkFilter("orders-main"),
                        spec)//
                .build();

        storage.save(config);

        final IndexConfiguration<String, String> loaded = storage.load();
        final IndexRuntimeConfiguration<String, String> runtimeConfiguration = loaded
                .resolveRuntimeConfiguration(registry);

        assertEquals(List.of(spec), loaded.getEncodingChunkFilterSpecs());
        assertEquals(List.of(spec), loaded.getDecodingChunkFilterSpecs());

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
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
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
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorShortString())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("spec-roundtrip-test")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInActivePartition(2)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(3)//
                .withMaxNumberOfKeysInIndexBuffer(9)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(11)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfStableSegmentMaintenanceThreads(7)//
                .withContextLoggingEnabled(false)//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .addEncodingFilter(new ChunkFilterDoNothing())//
                .addEncodingFilter(
                        () -> new TestEncodingChunkFilter("orders-archive"),
                        customSpec)//
                .addDecodingFilter(
                        () -> new TestDecodingChunkFilter("orders-archive"),
                        customSpec)//
                .addDecodingFilter(new ChunkFilterDoNothing())//
                .build();

        storage.save(config);

        final IndexConfiguration<String, String> loaded = storage.load();

        assertEquals(config.getEncodingChunkFilterSpecs(),
                loaded.getEncodingChunkFilterSpecs());
        assertEquals(config.getDecodingChunkFilterSpecs(),
                loaded.getDecodingChunkFilterSpecs());
        assertNotSame(config.getEncodingChunkFilterSpecs(),
                loaded.getEncodingChunkFilterSpecs());
        assertNotSame(config.getDecodingChunkFilterSpecs(),
                loaded.getDecodingChunkFilterSpecs());
        assertNotSame(config.getEncodingChunkFilterSpecs().get(1),
                loaded.getEncodingChunkFilterSpecs().get(1));
        assertNotSame(config.getDecodingChunkFilterSpecs().get(0),
                loaded.getDecodingChunkFilterSpecs().get(0));
    }

    private IndexConfiguration<String, String> buildConf() {
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        return IndexConfiguration.<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(typeDescriptor)//
                .withValueTypeDescriptor(typeDescriptor)//
                .withName("index-config-storage-test")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInActivePartition(2)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(3)//
                .withMaxNumberOfKeysInIndexBuffer(9)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(11)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfStableSegmentMaintenanceThreads(7)//
                .withContextLoggingEnabled(false)//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
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
