package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexPropertiesSchemaTest {

    private MemDirectory directory;
    private AsyncDirectory asyncDirectory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        asyncDirectory = AsyncDirectoryAdapter.wrap(directory);
    }

    @AfterEach
    void tearDown() {
        asyncDirectory = null;
        directory = null;
    }

    @Test
    void segmentSchemaAddsDefaultsAndMetadata() {
        final PropertyStore store = PropertyStoreimpl.fromAsyncDirectory(
                asyncDirectory, "manifest.txt", false);

        IndexPropertiesSchema.SEGMENT_SCHEMA.ensure(store);

        final PropertyView view = store.snapshot();
        assertEquals(IndexPropertiesSchema.CURRENT_SCHEMA_VERSION,
                view.getInt(IndexPropertiesSchema.SCHEMA_VERSION_KEY));
        assertEquals(0L, view.getLong(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE));
        assertEquals(0L, view.getLong(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_MAIN_INDEX));
        assertEquals(0L, view.getLong(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_SCARCE_INDEX));
        assertEquals(0, view.getInt(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_SEGMENT_CACHE_DELTA_FILES));
        assertEquals(0L, view.getLong(
                IndexPropertiesSchema.SegmentKeys.SEGMENT_VERSION));
        final String requiredKeys = view
                .getString(IndexPropertiesSchema.REQUIRED_KEYS_KEY);
        assertNotNull(requiredKeys);
        assertTrue(requiredKeys.contains(
                IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE));
    }

    @Test
    void indexConfigurationSchemaAddsDefaultsAndMetadata() {
        final PropertyStore store = PropertyStoreimpl.fromAsyncDirectory(
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
        assertEquals(IndexPropertiesSchema.CURRENT_SCHEMA_VERSION,
                view.getInt(IndexPropertiesSchema.SCHEMA_VERSION_KEY));
        assertEquals(IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                view.getInt(IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE));
        final int expectedWriteCache = IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE
                / 2;
        final int expectedWriteCacheDuringMaintenance = Math.max(
                expectedWriteCache * 2, expectedWriteCache + 1);
        assertEquals(expectedWriteCache,
                view.getInt(IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE));
        assertEquals(expectedWriteCacheDuringMaintenance, view.getInt(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE));
        assertEquals(IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_CACHE,
                view.getInt(IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_KEYS_IN_CACHE));
        assertEquals(IndexConfigurationContract.MAX_NUMBER_OF_DELTA_CACHE_FILES,
                view.getInt(IndexPropertiesSchema.IndexConfigurationKeys.PROP_MAX_NUMBER_OF_DELTA_CACHE_FILES));
        assertEquals(
                IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS,
                view.getInt(
                        IndexPropertiesSchema.IndexConfigurationKeys.PROP_NUMBER_OF_REGISTRY_LIFECYCLE_THREADS));
        assertEquals("",
                view.getString(IndexPropertiesSchema.IndexConfigurationKeys.PROP_ENCODING_CHUNK_FILTERS));
        assertEquals("",
                view.getString(IndexPropertiesSchema.IndexConfigurationKeys.PROP_DECODING_CHUNK_FILTERS));
        final String requiredKeys = view
                .getString(IndexPropertiesSchema.REQUIRED_KEYS_KEY);
        assertNotNull(requiredKeys);
        assertTrue(requiredKeys.contains(
                IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS));
    }

    @Test
    void indexConfigurationSchemaRejectsMissingRequiredKeys() {
        final PropertyStore store = PropertyStoreimpl.fromAsyncDirectory(
                asyncDirectory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);

        assertThrows(IllegalStateException.class,
                () -> IndexPropertiesSchema.INDEX_CONFIGURATION_SCHEMA
                        .ensure(store));
    }
}
