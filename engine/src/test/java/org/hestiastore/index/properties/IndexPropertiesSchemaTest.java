package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexPropertiesSchemaTest {

    private MemDirectory directory;
    private Directory asyncDirectory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        asyncDirectory = directory;
    }

    @AfterEach
    void tearDown() {
        asyncDirectory = null;
        directory = null;
    }

    @Test
    void segmentSchemaAddsDefaultsAndMetadata() {
        final PropertyStore store = PropertyStoreImpl
                .fromDirectory(asyncDirectory, "manifest.txt", false);

        IndexPropertiesSchema.SEGMENT_SCHEMA.ensure(store);

        final PropertyView view = store.snapshot();
        assertEquals(IndexPropertiesSchema.CURRENT_SEGMENT_SCHEMA_VERSION,
                view.getInt(IndexPropertiesSchema.SCHEMA_VERSION_KEY));
        assertEquals(0L, view.getLong(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE));
        assertEquals(0L, view.getLong(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_MAIN_INDEX));
        assertEquals(0L, view.getLong(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_SCARCE_INDEX));
        assertEquals(0, view.getInt(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_SEGMENT_CACHE_DELTA_FILES));
        assertEquals(0L, view
                .getLong(IndexPropertiesSchema.SegmentKeys.SEGMENT_VERSION));
        final String requiredKeys = view
                .getString(IndexPropertiesSchema.REQUIRED_KEYS_KEY);
        assertNotNull(requiredKeys);
        assertTrue(requiredKeys.contains(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE));
    }

    @Test
    void indexConfigurationSchemaAddsDefaultsAndMetadata() {
        final PropertyStore store = PropertyStoreImpl.fromDirectory(
                asyncDirectory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    Long.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    "test-key-descriptor");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    "test-value-descriptor");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "index-schema-test");
        }

        IndexPropertiesSchema.INDEX_CONFIGURATION_SCHEMA.ensure(store);

        final PropertyView view = store.snapshot();
        assertEquals(
                IndexPropertiesSchema.CURRENT_INDEX_CONFIGURATION_SCHEMA_VERSION,
                view.getInt(IndexPropertiesSchema.SCHEMA_VERSION_KEY));
        assertEquals(
                IndexConfigurationContract.DEFAULT_SEGMENT_CACHE_KEY_LIMIT,
                view.getInt(
                        IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE));
        final int expectedSegmentWriteCacheKeyLimit = IndexConfigurationContract.DEFAULT_SEGMENT_CACHE_KEY_LIMIT
                / 2;
        final int expectedMaintenanceLimit = Math.max(
                expectedSegmentWriteCacheKeyLimit * 2,
                expectedSegmentWriteCacheKeyLimit + 1);
        assertEquals(expectedSegmentWriteCacheKeyLimit, view.getInt(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT));
        assertEquals(expectedMaintenanceLimit, view.getInt(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE));
        assertEquals(expectedMaintenanceLimit
                * IndexConfigurationContract.DEFAULT_CACHED_SEGMENT_LIMIT,
                view.getInt(
                        IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_BUFFERED_WRITE_KEY_LIMIT));
        assertEquals(IndexConfigurationContract.DEFAULT_DELTA_CACHE_FILE_LIMIT,
                view.getInt(
                        IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES));
        assertEquals(
                IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS,
                view.getInt(
                        IndexPropertiesSchema.IndexConfigurationKeys.PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS));
        assertEquals("", view.getString(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS));
        assertEquals("", view.getString(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS));
        final String requiredKeys = view
                .getString(IndexPropertiesSchema.REQUIRED_KEYS_KEY);
        assertNotNull(requiredKeys);
        assertTrue(requiredKeys.contains(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS));
    }

    @Test
    void indexConfigurationSchemaRejectsMissingRequiredKeys() {
        final PropertyStore store = PropertyStoreImpl.fromDirectory(
                asyncDirectory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);

        assertThrows(IllegalStateException.class,
                () -> IndexPropertiesSchema.INDEX_CONFIGURATION_SCHEMA
                        .ensure(store));
    }

    @Test
    void indexConfigurationSchemaRejectsLegacyMaintenanceKeys() {
        final PropertyStore store = PropertyStoreImpl.fromDirectory(
                asyncDirectory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    Long.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    "test-key-descriptor");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    "test-value-descriptor");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "legacy-maintenance-schema-test");
            writer.setString("segmentMaintenanceAutoEnabled", "false");
            writer.setString("segmentIndexMaintenanceThreads", "6");
        }

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> IndexPropertiesSchema.INDEX_CONFIGURATION_SCHEMA
                        .ensure(store));
        assertTrue(error.getMessage().contains(
                "Unsupported legacy properties for 'index-configuration'"));
    }

    @Test
    void indexConfigurationSchemaRejectsSchemaVersionOne() {
        final PropertyStore store = PropertyStoreImpl.fromDirectory(
                asyncDirectory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    Long.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    "test-key-descriptor");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    "test-value-descriptor");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "schema-version-one-test");
            writer.setInt(IndexPropertiesSchema.SCHEMA_VERSION_KEY, 1);
        }

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> IndexPropertiesSchema.INDEX_CONFIGURATION_SCHEMA
                        .ensure(store));
        assertTrue(error.getMessage().contains(
                "Unsupported schema version 1 for 'index-configuration'; expected 2"));
    }
}
