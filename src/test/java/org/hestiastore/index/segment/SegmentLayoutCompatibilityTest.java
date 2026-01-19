package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SegmentLayoutCompatibilityTest extends AbstractSegmentTest {

    private final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void segment_roundTrip_supports_flat_and_root_layouts(
            final boolean useSegmentRoot) {
        final MemDirectory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentConf segmentConf = new SegmentConf(8, 16, 16, 4, null,
                null, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final List<Entry<Integer, String>> entries = List.of(Entry.of(1, "one"),
                Entry.of(2, "two"));

        try (Segment<Integer, String> segment = Segment.<Integer, String>builder()//
                .withAsyncDirectory(asyncDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyDescriptor)//
                .withValueTypeDescriptor(valueDescriptor)//
                .withSegmentConf(segmentConf)//
                .withSegmentRootDirectoryEnabled(useSegmentRoot)//
                .build()) {
            writeEntries(segment, entries);
            final long flatFileCount = directory.getFileNames().count();
            if (useSegmentRoot) {
                assertEquals(0, flatFileCount,
                        "Expected segment files under a subdirectory.");
            } else {
                assertTrue(flatFileCount > 0,
                        "Expected flat segment files in base directory.");
            }
        }

        try (Segment<Integer, String> reopened = Segment.<Integer, String>builder()//
                .withAsyncDirectory(asyncDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyDescriptor)//
                .withValueTypeDescriptor(valueDescriptor)//
                .withSegmentConf(segmentConf)//
                .withSegmentRootDirectoryEnabled(useSegmentRoot)//
                .build()) {
            verifySegmentSearch(reopened, entries);
        }
    }

    @Test
    void compaction_updates_active_pointer_in_root_layout() {
        final MemDirectory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentConf segmentConf = new SegmentConf(8, 16, 16, 4, null,
                null, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final List<Entry<Integer, String>> entries = List.of(Entry.of(1, "one"),
                Entry.of(2, "two"));

        try (Segment<Integer, String> segment = Segment
                .<Integer, String>builder()//
                .withAsyncDirectory(asyncDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyDescriptor)//
                .withValueTypeDescriptor(valueDescriptor)//
                .withSegmentConf(segmentConf)//
                .withSegmentRootDirectoryEnabled(true)//
                .build()) {
            writeEntries(segment, entries);
            final SegmentResult<java.util.concurrent.CompletionStage<Void>> result = segment
                    .compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            result.getValue().toCompletableFuture().join();
        }

        final AsyncDirectory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentDirectoryPointer pointer = new SegmentDirectoryPointer(
                rootDirectory, new SegmentDirectoryLayout(segmentId));
        assertEquals("v2", pointer.readActiveDirectory());
    }

    @Test
    void recovery_promotes_prepared_directory_after_pointer_rewind() {
        final MemDirectory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentConf segmentConf = new SegmentConf(8, 16, 16, 4, null,
                null, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final List<Entry<Integer, String>> entries = List.of(Entry.of(1, "one"),
                Entry.of(2, "two"));

        try (Segment<Integer, String> segment = Segment
                .<Integer, String>builder()//
                .withAsyncDirectory(asyncDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyDescriptor)//
                .withValueTypeDescriptor(valueDescriptor)//
                .withSegmentConf(segmentConf)//
                .withSegmentRootDirectoryEnabled(true)//
                .build()) {
            writeEntries(segment, entries);
            final SegmentResult<java.util.concurrent.CompletionStage<Void>> result = segment
                    .compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            result.getValue().toCompletableFuture().join();
        }

        final AsyncDirectory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                segmentId);
        final SegmentDirectoryPointer pointer = new SegmentDirectoryPointer(
                rootDirectory, layout);
        pointer.writeActiveDirectory("v1");
        final AsyncDirectory preparedDirectory = rootDirectory
                .openSubDirectory("v2").toCompletableFuture().join();
        final SegmentPropertiesManager preparedProperties = new SegmentPropertiesManager(
                preparedDirectory, segmentId);
        preparedProperties
                .setState(SegmentPropertiesManager.SegmentDataState.PREPARED);

        try (Segment<Integer, String> reopened = Segment
                .<Integer, String>builder()//
                .withAsyncDirectory(asyncDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyDescriptor)//
                .withValueTypeDescriptor(valueDescriptor)//
                .withSegmentConf(segmentConf)//
                .withSegmentRootDirectoryEnabled(true)//
                .build()) {
            verifySegmentSearch(reopened, entries);
        }

        assertEquals("v2", pointer.readActiveDirectory());
        final SegmentPropertiesManager refreshedProperties = new SegmentPropertiesManager(
                preparedDirectory, segmentId);
        assertEquals(SegmentPropertiesManager.SegmentDataState.ACTIVE,
                refreshedProperties.getState());
    }
}
