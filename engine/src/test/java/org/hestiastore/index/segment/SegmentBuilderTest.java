package org.hestiastore.index.segment;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;

/**
 * Class test invalid parameters of segment.
 */

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

/**
 * Class test invalid parameters of segment. There are two kind of tests 1)
 * verify that with methods are correctly validating input and second that
 * configuration is correctly used.
 */
class SegmentBuilderTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(27);
    private static final Directory DIRECTORY = new MemDirectory();
    private static final TypeDescriptor<String> VALUE_TYPE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final TypeDescriptor<Integer> KEY_TYPE_DESCRIPTOR = new TypeDescriptorInteger();

    @Test
    void test_directory_is_missing() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> Segment.<Integer, String>builder((Directory) null));
        assertEquals("Property 'directoryFacade' must not be null.",
                e.getMessage());
    }

    @Test
    void test_keyTypeDescriptor_is_missing() {
        final SegmentBuilder<Integer, String> builder = newBuilder()//
                .withId(SEGMENT_ID)//
                // .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withBloomFilterIndexSizeInBytes(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                e.getMessage());
    }

    @Test
    void test_maintenancePolicy_is_missing() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder(DIRECTORY)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()));

        final Exception e = assertThrows(IllegalArgumentException.class,
                builder::build);
        assertEquals("Property 'maintenancePolicy' must not be null.",
                e.getMessage());
    }

    @Test
    void test_valueTypeDescriptor_is_missing() {
        final SegmentBuilder<Integer, String> builder = newBuilder()//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                // .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withBloomFilterIndexSizeInBytes(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Property 'valueTypeDescriptor' must not be null.",
                e.getMessage());
    }

    @Test
    void test_withMaxNumberOfKeysInSegmentWriteCache_is_invalid() {
        final SegmentBuilder<Integer, String> builder = newBuilder();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withMaxNumberOfKeysInSegmentWriteCache(0));

        assertEquals(
                "Property 'maxNumberOfKeysInSegmentWriteCache' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_withMaxNumberOfKeysInSegmentChunk_is_invalid() {
        final SegmentBuilder<Integer, String> builder = newBuilder();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withMaxNumberOfKeysInSegmentChunk(-1));

        assertEquals(
                "Property 'maxNumberOfKeysInSegmentChunk' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_withDiskIoBufferSize_is_invalid() {
        final SegmentBuilder<Integer, String> builder = newBuilder();
        final Exception e1 = assertThrows(IllegalArgumentException.class,
                () -> builder.withDiskIoBufferSize(0));
        assertEquals("Property 'ioBufferSize' must be greater than 0",
                e1.getMessage());

        final Exception e2 = assertThrows(IllegalArgumentException.class,
                () -> builder.withDiskIoBufferSize(1000));
        assertEquals(
                "Propety 'ioBufferSize' must be divisible by 1024 (e.g., 1024, 2048, 4096). Got: '1000'",
                e2.getMessage());
    }

    @Test
    void test_encodingChunkFilters_are_missing() {
        final SegmentBuilder<Integer, String> builder = newBuilder()//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
        ;

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Property 'encodingChunkFilters' must not be empty.",
                e.getMessage());
    }

    @Test
    void test_decodingChunkFilters_are_missing() {
        final SegmentBuilder<Integer, String> builder = newBuilder()//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
        ;

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());
        assertEquals("Property 'decodingChunkFilters' must not be empty.",
                e.getMessage());
    }

    @Test
    void test_withEncodingChunkFilters_null() {
        final SegmentBuilder<Integer, String> builder = newBuilder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withEncodingChunkFilters(null));

        assertEquals("Property 'encodingChunkFilters' must not be null.",
                e.getMessage());
    }

    @Test
    void test_withEncodingChunkFilters_empty() {
        final SegmentBuilder<Integer, String> builder = newBuilder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withEncodingChunkFilters(List.of()));

        assertEquals("Property 'encodingChunkFilters' must not be empty.",
                e.getMessage());
    }

    @Test
    void test_withDecodingChunkFilters_null() {
        final SegmentBuilder<Integer, String> builder = newBuilder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withDecodingChunkFilters(null));

        assertEquals("Property 'decodingChunkFilters' must not be null.",
                e.getMessage());
    }

    @Test
    void test_withDecodingChunkFilters_empty() {
        final SegmentBuilder<Integer, String> builder = newBuilder();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.withDecodingChunkFilters(List.of()));

        assertEquals("Property 'decodingChunkFilters' must not be empty.",
                e.getMessage());
    }

    @Test
    void test_build_withProvidedChunkFilters() {
        final Segment<Integer, String> segment = Segment
                .<Integer, String>builder(DIRECTORY)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build().getValue();

        assertNotNull(segment);
        closeAndAwait(segment);
    }

    @Test
    void test_openWriterTx_writes_segment_and_builds() {
        final Directory directory = new MemDirectory();
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder(directory)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()));

        final SegmentFullWriterTx<Integer, String> tx = builder.openWriterTx();
        try (EntryWriter<Integer, String> writer = tx.open()) {
            writer.write(Entry.of(1, "a"));
            writer.write(Entry.of(2, "b"));
        }
        tx.commit();

        final Segment<Integer, String> segment = builder.build().getValue();
        final SegmentResult<String> first = segment.get(1);
        final SegmentResult<String> second = segment.get(2);
        assertEquals(SegmentResultStatus.OK, first.getStatus());
        assertEquals(SegmentResultStatus.OK, second.getStatus());
        assertEquals("a", first.getValue());
        assertEquals("b", second.getValue());
        closeAndAwait(segment);
    }

    @Test
    void test_build_sets_direct_maintenance_executor_when_missing()
            throws Exception {
        final Segment<Integer, String> segment = Segment
                .<Integer, String>builder(DIRECTORY)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build().getValue();

        final SegmentImpl<Integer, String> impl = (SegmentImpl<Integer, String>) segment;
        final Field field = SegmentImpl.class
                .getDeclaredField("maintenanceExecutor");
        field.setAccessible(true);
        final Object executor = field.get(impl);

        assertEquals(DirectExecutor.class, executor.getClass());
        closeAndAwait(segment);
    }

    @Test
    void builder_honors_active_version_from_properties() throws Exception {
        final Directory directory = new MemDirectory();
        final var asyncDirectory = directory;
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                segmentId);

        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                asyncDirectory, segmentId);
        propertiesManager.setVersion(2L);
        asyncDirectory.getFileWriter(layout.getIndexFileName(2)).close();

        final Segment<Integer, String> segment = Segment
                .<Integer, String>builder(directory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build().getValue();
        try {
            assertNotNull(segment);
            final SegmentImpl<Integer, String> impl = (SegmentImpl<Integer, String>) segment;
            final Field coreField = SegmentImpl.class.getDeclaredField("core");
            coreField.setAccessible(true);
            final SegmentCore<?, ?> core = (SegmentCore<?, ?>) coreField
                    .get(impl);
            final Field filesField = SegmentCore.class
                    .getDeclaredField("segmentFiles");
            filesField.setAccessible(true);
            final SegmentFiles<?, ?> files = (SegmentFiles<?, ?>) filesField
                    .get(core);
            assertEquals(2L, files.getActiveVersion());
        } finally {
            closeAndAwait(segment);
        }

        final SegmentPropertiesManager rootProperties = new SegmentPropertiesManager(
                asyncDirectory, segmentId);
        assertEquals(2L, rootProperties.getVersion());
    }

    private SegmentBuilder<Integer, String> newBuilder() {
        return Segment.<Integer, String>builder(DIRECTORY)
                .withMaintenancePolicy(SegmentMaintenancePolicy.none());
    }
}
