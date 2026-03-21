package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hestiastore.index.segmentregistry.SegmentTestFixtures.SEGMENT_DIR_NAME;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentTestFixtures.FailingRootDeleteDirectory;
import org.junit.jupiter.api.Test;

class SegmentRegistryFileSystemTest {

    @Test
    void segmentDirectoryExistsReflectsDirectoryPresence() {
        final MemDirectory directory = new MemDirectory();
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                directory);

        assertFalse(fileSystem.segmentDirectoryExists(SegmentId.of(1)));

        directory.mkdir(SEGMENT_DIR_NAME);

        assertTrue(fileSystem.segmentDirectoryExists(SegmentId.of(1)));
    }

    @Test
    void deleteSegmentFilesRemovesNestedDirectoryTree() {
        final MemDirectory directory = new MemDirectory();
        final Directory segmentDirectory = directory
                .openSubDirectory(SEGMENT_DIR_NAME);
        segmentDirectory.touch("data.bin");
        final Directory nestedDirectory = segmentDirectory
                .openSubDirectory("sub");
        nestedDirectory.touch("more.bin");

        final Directory asyncDirectory = directory;
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                asyncDirectory);

        assertTrue(fileSystem.deleteSegmentFiles(SegmentId.of(1)));

        assertFalse(directory.isFileExists(SEGMENT_DIR_NAME));
    }

    @Test
    void deleteSegmentFilesReturnsFalseWhenRootDirectoryRemains() {
        final FailingRootDeleteDirectory directory = new FailingRootDeleteDirectory(
                SEGMENT_DIR_NAME);
        directory.openSubDirectory(SEGMENT_DIR_NAME).touch("data.bin");
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                directory);

        assertFalse(fileSystem.deleteSegmentFiles(SegmentId.of(1)));
        assertTrue(directory.isFileExists(SEGMENT_DIR_NAME));
    }
}
