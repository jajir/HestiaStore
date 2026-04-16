package org.hestiastore.index.segmentindex.split;

import java.util.stream.Stream;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;

/**
 * Filesystem helper for prepared split segments.
 */
final class SegmentMaterializationFileSystem {

    private final Directory directoryFacade;

    SegmentMaterializationFileSystem(final Directory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
    }

    void ensureSegmentDirectory(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        directoryFacade.openSubDirectory(segmentId.getName());
    }

    void deletePreparedSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        deleteDirectory(segmentId.getName());
    }

    private void deleteDirectory(final String directoryName) {
        if (!directoryFacade.isFileExists(directoryName)) {
            return;
        }
        final Directory directory = directoryFacade
                .openSubDirectory(directoryName);
        clearDirectory(directory);
        try {
            directoryFacade.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            return;
        }
    }

    private void clearDirectory(final Directory directory) {
        try (Stream<String> entries = directory.getFileNames()) {
            entries.forEach(entry -> {
                if (tryDeleteFile(directory, entry)
                        || !exists(directory, entry)) {
                    return;
                }
                deleteSubDirectory(directory, entry);
            });
        }
    }

    private boolean tryDeleteFile(final Directory directory,
            final String fileName) {
        try {
            return directory.deleteFile(fileName);
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private boolean exists(final Directory directory, final String fileName) {
        try {
            return directory.isFileExists(fileName);
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    private void deleteSubDirectory(final Directory directory,
            final String directoryName) {
        try {
            final Directory subDirectory = directory
                    .openSubDirectory(directoryName);
            clearDirectory(subDirectory);
            directory.rmdir(directoryName);
        } catch (final RuntimeException ex) {
            return;
        }
    }
}
