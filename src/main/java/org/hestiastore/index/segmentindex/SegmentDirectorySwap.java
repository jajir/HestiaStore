package org.hestiastore.index.segmentindex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncFileReaderBlockingAdapter;
import org.hestiastore.index.directory.async.AsyncFileWriterBlockingAdapter;
import org.hestiastore.index.segment.SegmentId;

final class SegmentDirectorySwap {

    private static final String MARKER_SUFFIX = ".swap";
    private static final String MARKER_TEMP_SUFFIX = ".tmp";
    private static final String TEMP_SUFFIX = ".swap.dir";
    private static final String REPLACEMENT_KEY = "replacementSegmentId";
    private static final String MARKER_COMMENT = "Segment directory swap marker";

    private final AsyncDirectory directoryFacade;

    SegmentDirectorySwap(final AsyncDirectory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
    }

    void swap(final SegmentId segmentId, final SegmentId replacementSegmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(replacementSegmentId, "replacementSegmentId");
        recoverIfNeeded(segmentId);
        writeMarker(segmentId, replacementSegmentId);
        performSwap(segmentId.getName(), replacementSegmentId.getName());
        deleteMarker(segmentId);
    }

    void recoverIfNeeded(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final String markerFileName = markerFileName(segmentId);
        if (!exists(markerFileName)) {
            return;
        }
        final String replacementName = readReplacementName(markerFileName);
        if (replacementName == null || replacementName.isBlank()) {
            deleteMarker(segmentId);
            return;
        }
        if (exists(replacementName)) {
            performSwap(segmentId.getName(), replacementName);
            deleteMarker(segmentId);
            return;
        }
        final String currentName = segmentId.getName();
        final String tempName = tempDirectoryName(currentName);
        if (!exists(currentName) && exists(tempName)) {
            renameDirectory(tempName, currentName);
        } else if (exists(currentName) && exists(tempName)) {
            deleteDirectory(tempName);
        }
        deleteMarker(segmentId);
    }

    void deleteSegmentRootDirectory(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        deleteDirectory(segmentId.getName());
    }

    private void performSwap(final String currentName,
            final String replacementName) {
        final String tempName = tempDirectoryName(currentName);
        if (exists(replacementName)) {
            if (exists(currentName) && !exists(tempName)) {
                renameDirectory(currentName, tempName);
            }
            if (exists(currentName) && exists(tempName)) {
                deleteDirectory(currentName);
            }
            if (exists(replacementName)) {
                renameDirectory(replacementName, currentName);
            }
        }
        if (exists(tempName)) {
            deleteDirectory(tempName);
        }
    }

    private void renameDirectory(final String currentName,
            final String newName) {
        if (!exists(currentName)) {
            return;
        }
        directoryFacade.renameFileAsync(currentName, newName)
                .toCompletableFuture().join();
    }

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

    private void writeMarker(final SegmentId segmentId,
            final SegmentId replacementSegmentId) {
        final Properties properties = new Properties();
        properties.setProperty(REPLACEMENT_KEY,
                replacementSegmentId.getName());
        writeProperties(markerFileName(segmentId), properties);
    }

    private String readReplacementName(final String markerFileName) {
        if (!exists(markerFileName)) {
            return null;
        }
        final Properties properties = readProperties(markerFileName);
        final String value = properties.getProperty(REPLACEMENT_KEY);
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private void deleteMarker(final SegmentId segmentId) {
        final String markerFileName = markerFileName(segmentId);
        directoryFacade.deleteFileAsync(markerFileName).toCompletableFuture()
                .join();
    }

    private boolean exists(final String fileName) {
        return directoryFacade.isFileExistsAsync(fileName).toCompletableFuture()
                .join();
    }

    private String markerFileName(final SegmentId segmentId) {
        return segmentId.getName() + MARKER_SUFFIX;
    }

    private String tempDirectoryName(final String currentName) {
        return currentName + TEMP_SUFFIX;
    }

    private Properties readProperties(final String fileName) {
        final Properties properties = new Properties();
        try (FileReader reader = new AsyncFileReaderBlockingAdapter(
                directoryFacade.getFileReaderAsync(fileName).toCompletableFuture()
                        .join())) {
            final byte[] bytes = readAllBytes(reader);
            properties.load(new ByteArrayInputStream(bytes));
        } catch (final IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
        return properties;
    }

    private void writeProperties(final String fileName,
            final Properties properties) {
        final byte[] bytes = toBytes(properties);
        final String tmpFileName = fileName + MARKER_TEMP_SUFFIX;
        try (FileWriter writer = new AsyncFileWriterBlockingAdapter(
                directoryFacade.getFileWriterAsync(tmpFileName, Access.OVERWRITE)
                        .toCompletableFuture().join())) {
            writer.write(bytes);
        }
        directoryFacade.renameFileAsync(tmpFileName, fileName)
                .toCompletableFuture().join();
    }

    private byte[] readAllBytes(final FileReader reader) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] buffer = new byte[256];
        int read = reader.read(buffer);
        while (read != -1) {
            baos.write(buffer, 0, read);
            read = reader.read(buffer);
        }
        return baos.toByteArray();
    }

    private byte[] toBytes(final Properties properties) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            properties.store(baos, MARKER_COMMENT);
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }
}
