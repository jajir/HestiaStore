package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentRegistryFileSystemTest {

    @Test
    void segmentDirectoryExistsReflectsDirectoryPresence() {
        final MemDirectory directory = new MemDirectory();
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                AsyncDirectoryAdapter.wrap(directory));

        assertFalse(fileSystem.segmentDirectoryExists(SegmentId.of(1)));

        directory.mkdir("segment-00001");

        assertTrue(fileSystem.segmentDirectoryExists(SegmentId.of(1)));
    }

    @Test
    void hasAnySegmentDirectoriesUsesSegmentPattern() {
        final MemDirectory directory = new MemDirectory();
        directory.touch("random-file");
        directory.mkdir("tmp");
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                AsyncDirectoryAdapter.wrap(directory));

        assertFalse(fileSystem.hasAnySegmentDirectories());

        directory.mkdir("segment-00017");

        assertTrue(fileSystem.hasAnySegmentDirectories());
    }

    @Test
    void deleteSegmentFilesRemovesNestedDirectoryTree() {
        final MemDirectory directory = new MemDirectory();
        final Directory segmentDirectory = directory
                .openSubDirectory("segment-00001");
        segmentDirectory.touch("data.bin");
        final Directory nestedDirectory = segmentDirectory
                .openSubDirectory("sub");
        nestedDirectory.touch("more.bin");

        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                asyncDirectory);

        fileSystem.deleteSegmentFiles(SegmentId.of(1));

        assertFalse(directory.isFileExists("segment-00001"));
    }
}
