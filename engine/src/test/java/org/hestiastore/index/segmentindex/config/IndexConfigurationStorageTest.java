package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyWriter;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;

class IndexConfigurationStorageTest {

    private static final String LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE = "maxNumberOfKeysInSegmentWriteCache";
    private static final String LEGACY_MAX_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE = "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance";
    private static final String LEGACY_SEGMENT_MAINTENANCE_AUTO_ENABLED = "segmentMaintenanceAutoEnabled";
    private static final String LEGACY_SEGMENT_INDEX_MAINTENANCE_THREADS = "segmentIndexMaintenanceThreads";

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
}
