package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentRegistryFileSystemTest {

    @Test
    void segmentDirectoryExistsReflectsDirectoryPresence() {
        final MemDirectory directory = new MemDirectory();
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                directory);

        assertFalse(fileSystem.segmentDirectoryExists(SegmentId.of(1)));

        directory.mkdir("segment-00001");

        assertTrue(fileSystem.segmentDirectoryExists(SegmentId.of(1)));
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

        final Directory asyncDirectory = directory;
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                asyncDirectory);

        assertTrue(fileSystem.deleteSegmentFiles(SegmentId.of(1)));

        assertFalse(directory.isFileExists("segment-00001"));
    }

    @Test
    void deleteSegmentFilesReturnsFalseWhenRootDirectoryRemains() {
        final FailingRootDeleteDirectory directory = new FailingRootDeleteDirectory(
                "segment-00001");
        directory.openSubDirectory("segment-00001").touch("data.bin");
        final SegmentRegistryFileSystem fileSystem = new SegmentRegistryFileSystem(
                directory);

        assertFalse(fileSystem.deleteSegmentFiles(SegmentId.of(1)));
        assertTrue(directory.isFileExists("segment-00001"));
    }

    private static final class FailingRootDeleteDirectory extends MemDirectory {
        private final String failingDirectoryName;

        private FailingRootDeleteDirectory(final String failingDirectoryName) {
            this.failingDirectoryName = failingDirectoryName;
        }

        @Override
        public boolean rmdir(final String directoryName) {
            if (failingDirectoryName.equals(directoryName)) {
                throw new IllegalStateException(
                        "Simulated root delete failure.");
            }
            return super.rmdir(directoryName);
        }
    }
}
