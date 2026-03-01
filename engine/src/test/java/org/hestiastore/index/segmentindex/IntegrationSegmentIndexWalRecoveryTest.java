package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSegmentIndexWalRecoveryTest {

    @Test
    void reopenRecoversFromCorruptedWalTail() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("wal-recovery-it")//
                .withWal(Wal.builder().withEnabled(true).build())//
                .build();
        try (SegmentIndex<String, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put("k1", "v1");
            index.put("k2", "v2");
            index.flushAndWait();
        }
        final Directory walDirectory = directory.openSubDirectory("wal");
        final String segmentName = walDirectory.getFileNames()
                .filter(name -> name.endsWith(".wal")).findFirst()
                .orElseThrow();
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter(segmentName, Directory.Access.APPEND)) {
            writer.write(new byte[] { 0x01, 0x02, 0x03 });
        }

        try (SegmentIndex<String, String> reopened = SegmentIndex
                .open(directory)) {
            assertEquals("v1", reopened.get("k1"));
            assertEquals("v2", reopened.get("k2"));
        }
    }

    @Test
    void walDisabledDoesNotCreateWalDirectoryAndReportsZeroWalMetrics()
            throws IOException {
        final Path tempDir = Files.createTempDirectory("hestia-wal-disabled");
        try {
            final FsDirectory directory = new FsDirectory(tempDir.toFile());
            final IndexConfiguration<String, String> conf = IndexConfiguration
                    .<String, String>builder()//
                    .withKeyClass(String.class)//
                    .withValueClass(String.class)//
                    .withName("wal-disabled-it")//
                    .build();

            try (SegmentIndex<String, String> index = SegmentIndex
                    .create(directory, conf)) {
                index.put("k1", "v1");
                index.flushAndWait();
                final SegmentIndexMetricsSnapshot snapshot = index
                        .metricsSnapshot();
                assertFalse(snapshot.isWalEnabled());
                assertEquals(0L, snapshot.getWalAppendCount());
                assertEquals(0L, snapshot.getWalSyncCount());
                assertEquals(0L, snapshot.getWalRetainedBytes());
            }

            assertFalse(Files.exists(tempDir.resolve("wal")));
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private static void deleteRecursively(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw e;
        }
    }
}
