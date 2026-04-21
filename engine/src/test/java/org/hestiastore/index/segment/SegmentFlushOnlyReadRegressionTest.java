package org.hestiastore.index.segment;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAssertClosed;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentFlushOnlyReadRegressionTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @TempDir
    private File tempDir;

    @Test
    void get_missing_key_after_flush_without_compaction_does_not_require_main_sst() {
        final SegmentId segmentId = SegmentId.of(1);
        final Directory rootDirectory = new FsDirectory(tempDir);
        final Directory segmentDirectory = rootDirectory
                .openSubDirectory(segmentId.getName());
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                segmentId);
        final Segment<Integer, String> segment = Segment
                .<Integer, String>builder(segmentDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withDiskIoBufferSize(1024)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .withEncodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberWriting(),
                                new ChunkFilterCrc32Writing(),
                                new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberValidation(),
                                new ChunkFilterCrc32Validation(),
                                new ChunkFilterDoNothing()))//
                .build().getValue();
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "one").getStatus());
            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "two").getStatus());
            assertEquals(SegmentResultStatus.OK, segment.flush().getStatus());

            assertTrue(segmentDirectory.isFileExists(layout.getIndexFileName()),
                    "Freshly created segment should materialize an empty base SST.");

            final SegmentResult<String> result = assertDoesNotThrow(
                    () -> segment.get(99),
                    "Missing-key lookup on a flush-only segment must not try to open a non-existent base SST.");

            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertNull(result.getValue());
        } finally {
            closeAndAssertClosed(segment);
        }
    }
}
