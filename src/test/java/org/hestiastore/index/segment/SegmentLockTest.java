package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class SegmentLockTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);
    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @Test
    void build_fails_when_segment_is_locked() {
        final Directory directory = new MemDirectory();
        try (Segment<Integer, String> segment = newBuilder(directory).build()) {
            assertThrows(IllegalStateException.class,
                    () -> newBuilder(directory).build());
        }
    }

    @Test
    void close_releases_lock_file() {
        final Directory directory = new MemDirectory();
        final String lockFileName = new SegmentDirectoryLayout(SEGMENT_ID)
                .getLockFileName();

        try (Segment<Integer, String> segment = newBuilder(directory).build()) {
            assertTrue(directory.isFileExists(lockFileName));
        }

        assertFalse(directory.isFileExists(lockFileName));
    }

    private SegmentBuilder<Integer, String> newBuilder(
            final Directory directory) {
        return Segment.<Integer, String>builder()//
                .withAsyncDirectory(
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
}
