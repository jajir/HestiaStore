package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

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
        appendGarbageWalTail(directory);

        try (SegmentIndex<String, String> reopened = SegmentIndex
                .open(directory)) {
            assertEquals("v1", reopened.get("k1"));
            assertEquals("v2", reopened.get("k2"));
        }
    }

    @Test
    void reopenFailsFastWhenWalTailIsCorruptedAndPolicyIsFailFast() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("wal-recovery-fail-fast-it")//
                .withWal(Wal.builder().withEnabled(true)
                        .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)
                        .build())//
                .build();
        try (SegmentIndex<String, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put("k1", "v1");
            index.flushAndWait();
        }
        appendGarbageWalTail(directory);

        assertThrows(RuntimeException.class, () -> SegmentIndex.open(directory));
    }

    @Test
    void repeatedReopenWithCorruptedWalTailKeepsDeterministicState() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("wal-recovery-cycle-it")//
                .withWal(Wal.builder().withEnabled(true).build())//
                .build();
        final Map<String, String> expected = new HashMap<>();
        final int keySpace = 12;
        final int cycles = 6;

        for (int cycle = 0; cycle < cycles; cycle++) {
            if (cycle == 0) {
                try (SegmentIndex<String, String> index = SegmentIndex
                        .create(directory, conf)) {
                    applyCycleMutations(index, expected, cycle, keySpace);
                }
            } else {
                try (SegmentIndex<String, String> index = SegmentIndex
                        .open(directory)) {
                    applyCycleMutations(index, expected, cycle, keySpace);
                }
            }
            appendGarbageWalTail(directory);
        }

        try (SegmentIndex<String, String> reopened = SegmentIndex
                .open(directory)) {
            for (int i = 0; i < keySpace; i++) {
                final String key = "k-" + i;
                final String expectedValue = expected.get(key);
                if (expectedValue == null) {
                    assertNull(reopened.get(key));
                } else {
                    assertEquals(expectedValue, reopened.get(key));
                }
            }
        }
    }

    private static void applyCycleMutations(final SegmentIndex<String, String> index,
            final Map<String, String> expected, final int cycle,
            final int keySpace) {
        for (int i = 0; i < 24; i++) {
            final String key = "k-" + (i % keySpace);
            if ((cycle + i) % 5 == 0) {
                index.delete(key);
                expected.remove(key);
                continue;
            }
            final String value = "v-" + cycle + "-" + i;
            index.put(key, value);
            expected.put(key, value);
        }
        if ((cycle & 1) == 0) {
            index.flushAndWait();
        }
    }

    private static void appendGarbageWalTail(final Directory directory) {
        final Directory walDirectory = directory.openSubDirectory("wal");
        final String segmentName = walDirectory.getFileNames()
                .filter(name -> name.endsWith(".wal")).findFirst()
                .orElseThrow();
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter(segmentName, Directory.Access.APPEND)) {
            writer.write(new byte[] { 0x01, 0x02, 0x03 });
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
