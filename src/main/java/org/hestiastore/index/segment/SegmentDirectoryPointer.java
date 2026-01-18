package org.hestiastore.index.segment;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncFileReaderBlockingAdapter;
import org.hestiastore.index.directory.async.AsyncFileWriterBlockingAdapter;

/**
 * Manages the pointer file that stores the active segment directory name.
 */
final class SegmentDirectoryPointer {

    static final String ACTIVE_DIRECTORY_KEY = "activeDirectory";
    private static final String POINTER_FILE_COMMENT = "Segment directory pointer";
    private static final String TEMP_SUFFIX = ".tmp";

    private final AsyncDirectory rootDirectory;
    private final String pointerFileName;

    SegmentDirectoryPointer(final AsyncDirectory rootDirectory,
            final SegmentDirectoryLayout layout) {
        this.rootDirectory = Vldtn.requireNonNull(rootDirectory,
                "rootDirectory");
        this.pointerFileName = Vldtn.requireNonNull(layout, "layout")
                .getActivePointerFileName();
    }

    /**
     * Reads the active directory name from the pointer file.
     *
     * @return active directory name or null when not set
     */
    String readActiveDirectory() {
        if (!rootDirectory.isFileExistsAsync(pointerFileName)
                .toCompletableFuture().join()) {
            return null;
        }
        final Properties properties = readProperties();
        final String value = properties.getProperty(ACTIVE_DIRECTORY_KEY);
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    /**
     * Writes the active directory name into the pointer file.
     *
     * @param directoryName active directory name
     */
    void writeActiveDirectory(final String directoryName) {
        Vldtn.requireNonNull(directoryName, "directoryName");
        if (directoryName.isBlank()) {
            throw new IllegalArgumentException(
                    "directoryName must not be blank");
        }
        final Properties properties = new Properties();
        properties.setProperty(ACTIVE_DIRECTORY_KEY, directoryName);
        writeProperties(properties);
    }

    private Properties readProperties() {
        final Properties properties = new Properties();
        try (FileReader reader = new AsyncFileReaderBlockingAdapter(
                rootDirectory.getFileReaderAsync(pointerFileName)
                        .toCompletableFuture().join())) {
            final byte[] bytes = readAllBytes(reader);
            properties.load(new ByteArrayInputStream(bytes));
        } catch (final IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
        return properties;
    }

    private void writeProperties(final Properties properties) {
        final byte[] bytes = toBytes(properties);
        final String tmpFileName = pointerFileName + TEMP_SUFFIX;
        try (FileWriter writer = new AsyncFileWriterBlockingAdapter(
                rootDirectory.getFileWriterAsync(tmpFileName, Access.OVERWRITE)
                        .toCompletableFuture().join())) {
            writer.write(bytes);
        }
        rootDirectory.renameFileAsync(tmpFileName, pointerFileName)
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
            properties.store(baos, POINTER_FILE_COMMENT);
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }
}
