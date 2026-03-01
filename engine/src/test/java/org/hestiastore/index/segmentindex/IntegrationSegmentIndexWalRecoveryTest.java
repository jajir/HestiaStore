package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
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
    void reopenFailFastDoesNotMutateWalAcrossAttempts() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("wal-recovery-fail-fast-immutability-it")//
                .withWal(Wal.builder().withEnabled(true)
                        .withSegmentSizeBytes(96L)
                        .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)
                        .build())//
                .build();
        try (SegmentIndex<String, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 40; i++) {
                index.put("k-" + i, "v-" + i);
            }
            index.flushAndWait();
        }
        appendGarbageWalTail(directory);
        final Map<String, byte[]> expectedSnapshot = walSegmentSnapshot(directory);

        assertThrows(RuntimeException.class, () -> SegmentIndex.open(directory));
        assertWalSnapshotsEqual(expectedSnapshot, walSegmentSnapshot(directory));

        assertThrows(RuntimeException.class, () -> SegmentIndex.open(directory));
        assertWalSnapshotsEqual(expectedSnapshot, walSegmentSnapshot(directory));
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

    @Test
    void longRunCrashReopenCyclesWithRandomMutationsRemainDeterministic() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("wal-recovery-random-cycle-it")//
                .withWal(Wal.builder().withEnabled(true).build())//
                .build();
        final Map<String, String> expected = new HashMap<>();
        final Random random = new Random(42L);
        final int keySpace = 32;
        final int cycles = 20;

        for (int cycle = 0; cycle < cycles; cycle++) {
            if (cycle == 0) {
                try (SegmentIndex<String, String> index = SegmentIndex
                        .create(directory, conf)) {
                    applyRandomCycleMutations(index, expected, random, cycle,
                            keySpace, 40);
                }
            } else {
                try (SegmentIndex<String, String> index = SegmentIndex
                        .open(directory)) {
                    applyRandomCycleMutations(index, expected, random, cycle,
                            keySpace, 40);
                }
            }
            appendGarbageWalTail(directory);
        }

        try (SegmentIndex<String, String> reopened = SegmentIndex
                .open(directory)) {
            for (int i = 0; i < keySpace; i++) {
                final String key = "rk-" + i;
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

    private static void applyRandomCycleMutations(
            final SegmentIndex<String, String> index,
            final Map<String, String> expected, final Random random,
            final int cycle, final int keySpace, final int operationsPerCycle) {
        for (int i = 0; i < operationsPerCycle; i++) {
            final String key = "rk-" + random.nextInt(keySpace);
            if (random.nextInt(6) == 0) {
                index.delete(key);
                expected.remove(key);
            } else {
                final String value = "rv-" + cycle + "-" + i + "-"
                        + random.nextInt(1000);
                index.put(key, value);
                expected.put(key, value);
            }
            if (random.nextInt(11) == 0) {
                index.flushAndWait();
            }
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

    private static Map<String, byte[]> walSegmentSnapshot(
            final MemDirectory directory) {
        final MemDirectory walDirectory = (MemDirectory) directory
                .openSubDirectory("wal");
        final Map<String, byte[]> snapshot = new HashMap<>();
        walDirectory.getFileNames().filter(name -> name.endsWith(".wal"))
                .forEach(name -> snapshot.put(name,
                        walDirectory.getFileSequence(name).toByteArrayCopy()));
        return snapshot;
    }

    private static void assertWalSnapshotsEqual(final Map<String, byte[]> expected,
            final Map<String, byte[]> actual) {
        assertEquals(expected.keySet(), actual.keySet());
        for (final Map.Entry<String, byte[]> entry : expected.entrySet()) {
            final byte[] actualBytes = actual.get(entry.getKey());
            assertTrue(Arrays.equals(entry.getValue(), actualBytes),
                    "WAL segment bytes changed for " + entry.getKey());
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

    @Test
    void walRetentionPressureForcesCheckpointAndWritesKeepProgressing() {
        final MemDirectory directory = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
                .withSegmentSizeBytes(96L)//
                .withMaxBytesBeforeForcedCheckpoint(192L)//
                .build();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("wal-retention-pressure-it")//
                .withWal(wal)//
                .build();

        try (SegmentIndex<String, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 300; i++) {
                index.put("bp-" + i, "value-" + i);
            }
            final SegmentIndexMetricsSnapshot snapshot = index.metricsSnapshot();
            assertTrue(snapshot.isWalEnabled());
            assertTrue(snapshot.getWalAppendCount() >= 300L);
            assertTrue(snapshot.getWalCheckpointLsn() > 0L,
                    "Expected forced checkpoint under retention pressure.");
            assertTrue(snapshot.getWalDurableLsn() >= snapshot.getWalCheckpointLsn());
            assertEquals(0L, snapshot.getWalSyncFailureCount());
            assertTrue(snapshot.getWalRetainedBytes() <= wal
                    .getMaxBytesBeforeForcedCheckpoint()
                    + wal.getSegmentSizeBytes());
            assertEquals("value-299", index.get("bp-299"));
        }
    }

    @Test
    void walRetentionPressureWarningIsThrottled() {
        final TestLogAppender appender = TestLogAppender.attachWarnRootAppender();
        try {
            final MemDirectory directory = new MemDirectory();
            final Wal wal = Wal.builder()//
                    .withEnabled(true)//
                    .withSegmentSizeBytes(96L)//
                    .withMaxBytesBeforeForcedCheckpoint(192L)//
                    .build();
            final IndexConfiguration<String, String> conf = IndexConfiguration
                    .<String, String>builder()//
                    .withKeyClass(String.class)//
                    .withValueClass(String.class)//
                    .withName("wal-retention-pressure-log-throttle-it")//
                    .withWal(wal)//
                    .build();

            try (SegmentIndex<String, String> index = SegmentIndex
                    .create(directory, conf)) {
                for (int i = 0; i < 300; i++) {
                    index.put("throttle-" + i, "value-" + i);
                }
            }

            final long warningCount = appender.countMessageContaining(
                    "WAL retention pressure detected");
            assertEquals(1L, warningCount,
                    "Expected only one retention-pressure warning within throttle interval.");
        } finally {
            appender.detach();
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

    private static final class TestLogAppender extends AbstractAppender {

        private final LoggerContext loggerContext;
        private final List<LogEvent> events = new java.util.ArrayList<>();

        private TestLogAppender(final String name,
                final LoggerContext loggerContext) {
            super(name, null, PatternLayout.createDefaultLayout(), true,
                    Property.EMPTY_ARRAY);
            this.loggerContext = loggerContext;
        }

        static TestLogAppender attachWarnRootAppender() {
            final LoggerContext context = (LoggerContext) LogManager
                    .getContext(false);
            final Configuration configuration = context.getConfiguration();
            final String name = "wal-retention-pressure-test-appender-"
                    + System.nanoTime();
            final TestLogAppender appender = new TestLogAppender(name, context);
            appender.start();
            configuration.getRootLogger().addAppender(appender, Level.WARN,
                    null);
            context.updateLoggers();
            return appender;
        }

        void detach() {
            final Configuration configuration = loggerContext
                    .getConfiguration();
            configuration.getRootLogger().removeAppender(getName());
            stop();
            loggerContext.updateLoggers();
        }

        long countMessageContaining(final String fragment) {
            synchronized (events) {
                return events.stream()
                        .map(event -> event.getMessage().getFormattedMessage())
                        .filter(message -> message.contains(fragment)).count();
            }
        }

        @Override
        public void append(final LogEvent event) {
            synchronized (events) {
                events.add(event.toImmutable());
            }
        }
    }
}
