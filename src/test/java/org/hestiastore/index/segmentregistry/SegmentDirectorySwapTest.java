package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.directory.async.AsyncFileWriter;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentDirectorySwapTest {

    private MemDirectory directory;
    private AsyncDirectory asyncDirectory;
    private SegmentDirectorySwap swapper;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        asyncDirectory = AsyncDirectoryAdapter.wrap(directory);
        swapper = new SegmentDirectorySwap(asyncDirectory);
    }

    @AfterEach
    void tearDown() {
        swapper = null;
        asyncDirectory = null;
        directory = null;
    }

    @Test
    void swap_moves_replacement_directory_into_current() {
        final SegmentId currentId = SegmentId.of(1);
        final SegmentId replacementId = SegmentId.of(2);
        writeFile(currentId, "current.txt", "current");
        writeFile(replacementId, "replacement.txt", "replacement");

        swapper.swap(currentId, replacementId);

        assertTrue(exists(currentId.getName()));
        assertFalse(exists(replacementId.getName()));
        assertFalse(exists(markerName(currentId)));
        final List<String> currentFiles = listFiles(currentId);
        assertTrue(currentFiles.contains("replacement.txt"));
        assertFalse(currentFiles.contains("current.txt"));
    }

    @Test
    void recoverIfNeeded_finishes_swap_after_current_is_moved() {
        final SegmentId currentId = SegmentId.of(3);
        final SegmentId replacementId = SegmentId.of(4);
        writeFile(currentId, "current.txt", "current");
        writeFile(replacementId, "replacement.txt", "replacement");
        writeMarker(currentId, replacementId);

        asyncDirectory.renameFileAsync(currentId.getName(), tempName(currentId))
                .toCompletableFuture().join();

        swapper.recoverIfNeeded(currentId);

        assertTrue(exists(currentId.getName()));
        assertFalse(exists(replacementId.getName()));
        assertFalse(exists(markerName(currentId)));
        final List<String> currentFiles = listFiles(currentId);
        assertTrue(currentFiles.contains("replacement.txt"));
    }

    @Test
    void recoverIfNeeded_restores_current_when_replacement_missing() {
        final SegmentId currentId = SegmentId.of(5);
        final SegmentId replacementId = SegmentId.of(6);
        writeFile(currentId, "current.txt", "current");
        writeMarker(currentId, replacementId);

        asyncDirectory.renameFileAsync(currentId.getName(), tempName(currentId))
                .toCompletableFuture().join();

        swapper.recoverIfNeeded(currentId);

        assertTrue(exists(currentId.getName()));
        assertFalse(exists(markerName(currentId)));
        final List<String> currentFiles = listFiles(currentId);
        assertTrue(currentFiles.contains("current.txt"));
    }

    private void writeFile(final SegmentId segmentId, final String fileName,
            final String content) {
        final AsyncDirectory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        try (AsyncFileWriter writer = segmentDirectory
                .getFileWriterAsync(fileName, Access.OVERWRITE)
                .toCompletableFuture().join()) {
            writer.writeAsync(content.getBytes(StandardCharsets.US_ASCII))
                    .toCompletableFuture().join();
        }
    }

    private void writeMarker(final SegmentId segmentId,
            final SegmentId replacementId) {
        final Properties properties = new Properties();
        properties.setProperty("replacementSegmentId", replacementId.getName());
        final byte[] bytes = toBytes(properties);
        try (AsyncFileWriter writer = asyncDirectory
                .getFileWriterAsync(markerName(segmentId), Access.OVERWRITE)
                .toCompletableFuture().join()) {
            writer.writeAsync(bytes).toCompletableFuture().join();
        }
    }

    private byte[] toBytes(final Properties properties) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            properties.store(baos, "Segment directory swap marker");
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<String> listFiles(final SegmentId segmentId) {
        final AsyncDirectory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        try (Stream<String> files = segmentDirectory.getFileNamesAsync()
                .toCompletableFuture().join()) {
            return files.toList();
        }
    }

    private boolean exists(final String fileName) {
        return asyncDirectory.isFileExistsAsync(fileName).toCompletableFuture()
                .join();
    }

    private String markerName(final SegmentId segmentId) {
        return segmentId.getName() + ".swap";
    }

    private String tempName(final SegmentId segmentId) {
        return segmentId.getName() + ".swap.dir";
    }
}
