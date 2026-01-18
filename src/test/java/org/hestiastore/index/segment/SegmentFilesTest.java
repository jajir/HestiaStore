package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
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

    @Test
    void switchActiveDirectory_updates_active_directory_reference() {
        final AsyncDirectory rootDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                SegmentId.of(1));
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                rootDirectory,
                rootDirectory.openSubDirectory("v1")
                        .toCompletableFuture().join(),
                layout,
                "v1",
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        final AsyncDirectory v2Directory = rootDirectory
                .openSubDirectory("v2").toCompletableFuture().join();
        files.switchActiveDirectory("v2", v2Directory);

        assertEquals("v2", files.getActiveDirectoryName());
        assertSame(v2Directory, files.getAsyncDirectory());
    }

    @Test
    void copyWithDirectory_creates_new_instance() {
        final AsyncDirectory rootDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                SegmentId.of(1));
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                rootDirectory,
                rootDirectory.openSubDirectory("v1")
                        .toCompletableFuture().join(),
                layout,
                "v1",
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        final AsyncDirectory v3Directory = rootDirectory
                .openSubDirectory("v3").toCompletableFuture().join();
        final SegmentFiles<Integer, String> copy = files
                .copyWithDirectory("v3", v3Directory);

        assertEquals("v1", files.getActiveDirectoryName());
        assertEquals("v3", copy.getActiveDirectoryName());
        assertSame(v3Directory, copy.getAsyncDirectory());
    }
}
