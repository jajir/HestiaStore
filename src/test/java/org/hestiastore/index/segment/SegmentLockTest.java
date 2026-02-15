package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;

import java.io.File;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentLockTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);
    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @TempDir
    private File tempDir;

    @Test
    void build_fails_when_lock_file_exists_in_memory_directory() {
        final Directory directory = new MemDirectory();
        createLockFile(directory);

        assertEquals(SegmentBuildStatus.BUSY,
                newBuilder(directory).build().getStatus());
    }

    @Test
    void build_fails_when_lock_file_exists_in_filesystem_directory() {
        final Directory directory = new FsDirectory(tempDir);
        createLockFile(directory);

        assertEquals(SegmentBuildStatus.BUSY,
                newBuilder(directory).build().getStatus());
    }

    @Test
    void close_releases_lock_file() {
        final Directory directory = new MemDirectory();
        final String lockFileName = new SegmentDirectoryLayout(SEGMENT_ID)
                .getLockFileName();

        final Segment<Integer, String> segment = newBuilder(directory).build()
                .getValue();
        try {
            assertTrue(directory.isFileExists(lockFileName));
        } finally {
            closeAndAwait(segment);
        }

        assertFalse(directory.isFileExists(lockFileName));
    }

    private SegmentBuilder<Integer, String> newBuilder(
            final Directory directory) {
        return Segment.<Integer, String>builder(
                AsyncDirectoryAdapter.wrap(directory))//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withEncodingChunkFilters(//
                        List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(//
                        List.of(new ChunkFilterDoNothing()));
    }

    private void createLockFile(final Directory directory) {
        final String lockFileName = new SegmentDirectoryLayout(SEGMENT_ID)
                .getLockFileName();
        directory.touch(lockFileName);
    }
}
