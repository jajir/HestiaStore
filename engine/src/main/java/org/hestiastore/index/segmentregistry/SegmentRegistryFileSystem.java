package org.hestiastore.index.segmentregistry;

import java.util.stream.Stream;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;

/**
 * Filesystem operations used by {@link SegmentRegistryImpl}.
 */
final class SegmentRegistryFileSystem {

    private static final String SEGMENT_ID_ARG = "segmentId";
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
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return exists(segmentId.getName());
    }

    /**
     * Deletes all files and directories owned by the specified segment.
     *
     * @param segmentId segment identifier
     * @return {@code true} when the segment directory no longer exists
     */
    boolean deleteSegmentFiles(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return deleteDirectory(segmentId.getName());
    }

    /**
     * Ensures the segment directory exists.
     *
     * @param segmentId segment identifier
     */
    void ensureSegmentDirectory(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
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
     * @return {@code true} when the directory no longer exists
     */
    private boolean deleteDirectory(final String directoryName) {
        if (!exists(directoryName)) {
            return true;
        }
        final Directory directory = directoryFacade
                .openSubDirectory(directoryName);
        final boolean cleared = clearDirectory(directory);
        final boolean removed = removeDirectory(directoryFacade, directoryName);
        return cleared && removed && !exists(directoryName);
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
     * @return {@code true} when all entries were removed
     */
    private boolean clearDirectory(final Directory directory) {
        final String[] fileNames;
        try (Stream<String> files = directory.getFileNames()) {
            fileNames = files.toArray(String[]::new);
        } catch (final RuntimeException e) {
            return false;
        }
        boolean cleared = true;
        for (final String fileName : fileNames) {
            if (!deleteEntry(directory, fileName)) {
                cleared = false;
            }
        }
        return cleared;
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

    private boolean deleteEntry(final Directory directory,
            final String fileName) {
        boolean deletedAsFile = false;
        try {
            deletedAsFile = directory.deleteFile(fileName);
        } catch (final RuntimeException e) {
            deletedAsFile = false;
        }
        if (deletedAsFile) {
            return true;
        }
        if (!entryExists(directory, fileName)) {
            return true;
        }
        return deleteNestedDirectory(directory, fileName);
    }

    private boolean deleteNestedDirectory(final Directory directory,
            final String directoryName) {
        try {
            final Directory subDirectory = directory
                    .openSubDirectory(directoryName);
            final boolean cleared = clearDirectory(subDirectory);
            final boolean removed = removeDirectory(directory, directoryName);
            return cleared && removed && !entryExists(directory, directoryName);
        } catch (final RuntimeException e) {
            return false;
        }
    }

    private static boolean removeDirectory(final Directory directory,
            final String directoryName) {
        try {
            directory.rmdir(directoryName);
        } catch (final RuntimeException e) {
            return !entryExists(directory, directoryName);
        }
        return !entryExists(directory, directoryName);
    }

    private static boolean entryExists(final Directory directory,
            final String fileName) {
        try {
            return directory.isFileExists(fileName);
        } catch (final RuntimeException e) {
            return true;
        }
    }

}
