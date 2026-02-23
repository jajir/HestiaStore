package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentFilesTest {

    @Test
    void fileNamesUseVersionPrefix() {
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                new MemDirectory(), SegmentId.of(1),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()), 2L);

        assertEquals("v02-index.sst", files.getIndexFileName());
        assertEquals("v02-scarce.sst", files.getScarceFileName());
        assertEquals("v02-bloom-filter.bin", files.getBloomFilterFileName());
    }

    @Test
    void filterListsAreImmutable() {
        final ChunkFilterDoNothing encodingFilterToAdd = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing decodingFilterToAdd = new ChunkFilterDoNothing();
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                new MemDirectory(), SegmentId.of(1),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()), 1L);
        final List<ChunkFilter> encodingFilters = files
                .getEncodingChunkFilters();
        final List<ChunkFilter> decodingFilters = files
                .getDecodingChunkFilters();

        assertThrows(UnsupportedOperationException.class,
                () -> encodingFilters.add(encodingFilterToAdd));
        assertThrows(UnsupportedOperationException.class,
                () -> decodingFilters.add(decodingFilterToAdd));
    }

    @Test
    void constructorRejectsEmptyFilterLists() {
        final MemDirectory directory = new MemDirectory();
        final SegmentId segmentId = SegmentId.of(1);
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final List<ChunkFilter> emptyFilters = List.of();
        final List<ChunkFilter> decodingFilters = List
                .of(new ChunkFilterDoNothing());
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentFiles<>(directory, segmentId, keyDescriptor,
                        valueDescriptor, 1024, emptyFilters, decodingFilters,
                        1L));
    }

    @Test
    void switchActiveVersion_updates_active_version() {
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                SegmentId.of(1));
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                new MemDirectory(), layout, 1L, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        files.switchActiveVersion(2L);

        assertEquals(2L, files.getActiveVersion());
    }

    @Test
    void copyWithVersion_creates_new_instance() {
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                SegmentId.of(1));
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                new MemDirectory(), layout, 1L, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        final SegmentFiles<Integer, String> copy = files.copyWithVersion(3L);

        assertEquals(1L, files.getActiveVersion());
        assertEquals(3L, copy.getActiveVersion());
        assertSame(files.getDirectory(), copy.getDirectory());
    }
}
