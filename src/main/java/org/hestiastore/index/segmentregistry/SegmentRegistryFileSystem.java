package org.hestiastore.index.segmentregistry;

import java.util.stream.Stream;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;

/**
 * Filesystem operations used by {@link SegmentRegistryImpl}.
 */
final class SegmentRegistryFileSystem {

    private final Directory directoryFacade;

    /**
     * Creates a filesystem helper backed by the provided directory facade.
     *
     * @param directoryFacade directory abstraction for registry
     *                        operations
     */
    SegmentRegistryFileSystem(final Directory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
    }

    /**
     * Checks whether the directory for the specified segment exists.
     *
     * @param segmentId segment identifier
     * @return {@code true} when the segment directory exists
     */
    boolean segmentDirectoryExists(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return exists(segmentId.getName());
    }

    /**
     * Deletes all files and directories owned by the specified segment.
     *
     * @param segmentId segment identifier
     */
    void deleteSegmentFiles(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        deleteDirectory(segmentId.getName());
    }

    /**
     * Ensures the segment directory exists.
     *
     * @param segmentId segment identifier
     */
    void ensureSegmentDirectory(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        directoryFacade.openSubDirectory(segmentId.getName());
    }

    /**
     * Recursively removes a directory from the registry root.
     *
     * <p>
     * Cleanup is best effort: failures while deleting individual entries or
     * removing the root directory are swallowed.
     * </p>
     *
     * @param directoryName directory to remove
     */
    private void deleteDirectory(final String directoryName) {
        if (!exists(directoryName)) {
            return;
        }
        final Directory directory = directoryFacade
                .openSubDirectory(directoryName);
        clearDirectory(directory);
        try {
            directoryFacade.rmdir(directoryName);
        } catch (final RuntimeException e) {
            // Best-effort cleanup.
        }
    }

    /**
     * Recursively clears all entries from a directory.
     *
     * <p>
     * Each entry is first deleted as a file; if that fails and the entry still
     * exists, it is treated as a nested directory and removed recursively.
     * Failures are ignored to preserve best-effort semantics.
     * </p>
     *
     * @param directory directory to clear
     */
    private void clearDirectory(final Directory directory) {
        try (Stream<String> files = directory.getFileNames()) {
            files.forEach(fileName -> {
                boolean deleted = false;
                try {
                    deleted = directory.deleteFile(fileName);
                    if (deleted) {
                        return;
                    }
                } catch (final RuntimeException e) {
                    // fall through to directory cleanup
                }
                try {
                    if (!directory.isFileExists(fileName)) {
                        return;
                    }
                } catch (final RuntimeException e) {
                    return;
                }
                try {
                    final Directory subDirectory = directory
                            .openSubDirectory(fileName);
                    clearDirectory(subDirectory);
                    directory.rmdir(fileName);
                } catch (final RuntimeException e) {
                    // Best-effort cleanup.
                }
            });
        }
    }

    /**
     * Checks whether a file or directory exists in the registry root.
     *
     * @param fileName entry name
     * @return {@code true} when the entry exists
     */
    private boolean exists(final String fileName) {
        return directoryFacade.isFileExists(fileName);
    }

}
