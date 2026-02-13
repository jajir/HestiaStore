package org.hestiastore.index.segmentregistry;

import java.util.stream.Stream;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.SegmentId;

/**
 * Filesystem operations used by {@link SegmentRegistryImpl}.
 */
final class SegmentRegistryFileSystem {
    private final AsyncDirectory directoryFacade;

    /**
     * Creates a filesystem helper backed by the provided directory facade.
     *
     * @param directoryFacade asynchronous directory abstraction for registry
     *                        operations
     */
    SegmentRegistryFileSystem(final AsyncDirectory directoryFacade) {
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
        directoryFacade.openSubDirectory(segmentId.getName())
                .toCompletableFuture().join();
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
        final AsyncDirectory directory = directoryFacade
                .openSubDirectory(directoryName).toCompletableFuture().join();
        clearDirectory(directory);
        try {
            directoryFacade.rmdir(directoryName).toCompletableFuture().join();
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
    private void clearDirectory(final AsyncDirectory directory) {
        try (Stream<String> files = directory.getFileNamesAsync()
                .toCompletableFuture().join()) {
            files.forEach(fileName -> {
                boolean deleted = false;
                try {
                    deleted = directory.deleteFileAsync(fileName)
                            .toCompletableFuture().join();
                    if (deleted) {
                        return;
                    }
                } catch (final RuntimeException e) {
                    // fall through to directory cleanup
                }
                try {
                    if (!directory.isFileExistsAsync(fileName)
                            .toCompletableFuture().join()) {
                        return;
                    }
                } catch (final RuntimeException e) {
                    return;
                }
                try {
                    final AsyncDirectory subDirectory = directory
                            .openSubDirectory(fileName).toCompletableFuture()
                            .join();
                    clearDirectory(subDirectory);
                    directory.rmdir(fileName).toCompletableFuture().join();
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
        return directoryFacade.isFileExistsAsync(fileName).toCompletableFuture()
                .join();
    }

}
