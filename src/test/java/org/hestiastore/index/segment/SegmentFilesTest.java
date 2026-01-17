package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class SegmentFilesTest {

    @Test
    void fileNamesUseSegmentIdPrefix() {
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()), SegmentId.of(1),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        final String base = files.getSegmentIdName();
        assertEquals(base + ".index", files.getIndexFileName());
        assertEquals(base + ".scarce", files.getScarceFileName());
        assertEquals(base + ".bloom-filter", files.getBloomFilterFileName());
    }

    @Test
    void filterListsAreImmutable() {
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()), SegmentId.of(1),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        assertThrows(UnsupportedOperationException.class,
                () -> files.getEncodingChunkFilters()
                        .add(new ChunkFilterDoNothing()));
        assertThrows(UnsupportedOperationException.class,
                () -> files.getDecodingChunkFilters()
                        .add(new ChunkFilterDoNothing()));
    }

    @Test
    void constructorRejectsEmptyFilterLists() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentFiles<>(
                        AsyncDirectoryAdapter.wrap(new MemDirectory()),
                        SegmentId.of(1), new TypeDescriptorInteger(),
                        new TypeDescriptorShortString(), 1024, List.of(),
                        List.of(new ChunkFilterDoNothing())));
    }
}
