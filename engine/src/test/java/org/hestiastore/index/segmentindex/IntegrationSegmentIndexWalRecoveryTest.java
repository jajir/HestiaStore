package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.junit.jupiter.api.Test;

class IntegrationSegmentIndexWalRecoveryTest {

    @Test
    void crashReopenRecoversWalBufferedWritesWithoutSplit() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = integerWalRecoveryConfig(
                "wal-buffered-crash-recovery-it", false);
        final List<Entry<Integer, String>> expected = expectedStableOverlayEntries();

        final MemDirectory crashSnapshot;
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            seedStableSegment(index);
            applyOverlayMutations(index);
            assertIntegerIndexSnapshot(index, expected);

            crashSnapshot = copyDirectoryWithoutLocks(directory);
        }

        try (SegmentIndex<Integer, String> reopened = SegmentIndex
                .open(crashSnapshot)) {
            assertEquals("overlay-5", reopened.get(5));
            assertNull(reopened.get(18));
            assertEquals("overlay-44", reopened.get(44));
            assertEquals("overlay-49", reopened.get(49));
            assertIntegerIndexSnapshot(reopened, expected);
            assertEquals(1, reopened.metricsSnapshot().getSegmentCount());
        }
    }

    @Test
    void crashReopenCleansUnpublishedSplitChildrenAndReplaysWalIntoParentRoute() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = integerWalRecoveryConfig(
                "wal-split-orphan-children-recovery-it", false);
        final List<Entry<Integer, String>> expected = expectedStableOverlayEntries();

        final MemDirectory crashSnapshot;
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            seedStableSegment(index);
            assertEquals(1, index.metricsSnapshot().getSegmentCount());
            applyOverlayMutations(index);

            crashSnapshot = copyDirectoryWithoutLocks(directory);
        }

        final String parentSegmentDirectory = SegmentId.of(0).getName();
        final String orphanChildLowerDirectory = SegmentId.of(1).getName();
        final String orphanChildUpperDirectory = SegmentId.of(2).getName();
        copyDirectoryRecursively(
                (MemDirectory) crashSnapshot
                        .openSubDirectory(parentSegmentDirectory),
                (MemDirectory) crashSnapshot
                        .openSubDirectory(orphanChildLowerDirectory));
        copyDirectoryRecursively(
                (MemDirectory) crashSnapshot
                        .openSubDirectory(parentSegmentDirectory),
                (MemDirectory) crashSnapshot
                        .openSubDirectory(orphanChildUpperDirectory));
        assertTrue(crashSnapshot.isFileExists(orphanChildLowerDirectory));
        assertTrue(crashSnapshot.isFileExists(orphanChildUpperDirectory));

        try (SegmentIndex<Integer, String> reopened = SegmentIndex
                .open(crashSnapshot)) {
            assertEquals("overlay-5", reopened.get(5));
            assertNull(reopened.get(18));
            assertEquals("overlay-44", reopened.get(44));
            assertEquals("overlay-49", reopened.get(49));
            assertIntegerIndexSnapshot(reopened, expected);
            assertEquals(1, reopened.metricsSnapshot().getSegmentCount(),
                    "Interrupted split artifacts must not publish child routes.");
        }

        assertFalse(crashSnapshot.isFileExists(orphanChildLowerDirectory));
        assertFalse(crashSnapshot.isFileExists(orphanChildUpperDirectory));
        assertTrue(crashSnapshot.isFileExists(parentSegmentDirectory));
    }

    @Test
    void flushAndWaitKeepsCrashReopenSnapshotConsistentForBufferedWrites() {
        assertCrashReopenAfterMaintenanceBoundary(
                "wal-flush-split-boundary-it",
                SegmentIndex::flushAndWait);
    }

    @Test
    void compactAndWaitKeepsCrashReopenSnapshotConsistentForBufferedWrites() {
        assertCrashReopenAfterMaintenanceBoundary(
                "wal-compact-split-boundary-it",
                SegmentIndex::compactAndWait);
    }

    @Test
    void closeKeepsReopenSnapshotConsistentForBufferedWrites() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = integerWalRecoveryConfig(
                "wal-close-boundary-it", false);
        final List<Entry<Integer, String>> expected = expectedStableOverlayEntries();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            seedStableSegment(index);
            applyOverlayMutations(index);
        }

        try (SegmentIndex<Integer, String> reopened = SegmentIndex
                .open(directory)) {
            assertIntegerIndexSnapshot(reopened, expected);
            assertEquals(1, reopened.metricsSnapshot().getSegmentCount());
        }
    }

    @Test
    void reopenRecoversFromCorruptedWalTail() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("wal-recovery-it")//
                .withWal(Wal.builder().build())//
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
                .withWal(Wal.builder()
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
                .withWal(Wal.builder()
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
                .withWal(Wal.builder().build())//
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
                .withWal(Wal.builder().build())//
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

    private static void assertCrashReopenAfterMaintenanceBoundary(
            final String indexName,
            final Consumer<SegmentIndex<Integer, String>> maintenanceAction) {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = integerWalRecoveryConfig(
                indexName, true);
        final List<Entry<Integer, String>> expected = expectedStableOverlayEntries();

        final MemDirectory crashSnapshot;
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            seedStableSegment(index);
            applyOverlayMutations(index);

            maintenanceAction.accept(index);
            awaitCondition(() -> {
                final SegmentIndexMetricsSnapshot snapshot = index
                        .metricsSnapshot();
                return snapshot.getSplitInFlightCount() == 0
                        && snapshot.getDrainInFlightCount() == 0
                        && snapshot.getImmutableRunCount() == 0
                        && snapshot.getDrainingPartitionCount() == 0;
            }, 10_000L);
            assertIntegerIndexSnapshot(index, expected);

            crashSnapshot = copyDirectoryWithoutLocks(directory);
        }

        try (SegmentIndex<Integer, String> reopened = SegmentIndex
                .open(crashSnapshot)) {
            assertIntegerIndexSnapshot(reopened, expected);
        }
    }

    private static IndexConfiguration<Integer, String> integerWalRecoveryConfig(
            final String indexName,
            final boolean backgroundMaintenanceAutoEnabled) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName(indexName)//
                .withWal(Wal.builder().build())//
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInActivePartition(64) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(96) //
                .withMaxNumberOfKeysInIndexBuffer(192) //
                .withMaxNumberOfKeysInPartitionBeforeSplit(512) //
                .withMaxNumberOfKeysInSegment(128) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withBackgroundMaintenanceAutoEnabled(
                        backgroundMaintenanceAutoEnabled) //
                .build();
    }

    private static List<Entry<Integer, String>> expectedStableOverlayEntries() {
        return IntStream.concat(IntStream.range(0, 48), IntStream.of(49))
                .filter(key -> key != 18).mapToObj(key -> {
                    if (key == 5) {
                        return Entry.of(key, "overlay-5");
                    }
                    if (key == 44) {
                        return Entry.of(key, "overlay-44");
                    }
                    if (key == 49) {
                        return Entry.of(key, "overlay-49");
                    }
                    return Entry.of(key, "stable-" + key);
                }).toList();
    }

    private static void seedStableSegment(
            final SegmentIndex<Integer, String> index) {
        for (int i = 0; i < 48; i++) {
            index.put(i, "stable-" + i);
        }
        index.flushAndWait();
        awaitCondition(() -> index.metricsSnapshot().getSegmentCount() == 1
                && index.metricsSnapshot().getSplitInFlightCount() == 0
                && index.metricsSnapshot().getDrainInFlightCount() == 0,
                10_000L);
    }

    private static void applyOverlayMutations(
            final SegmentIndex<Integer, String> index) {
        index.put(5, "overlay-5");
        index.delete(18);
        index.put(44, "overlay-44");
        index.put(49, "overlay-49");
    }

    private static void assertIntegerIndexSnapshot(
            final SegmentIndex<Integer, String> index,
            final List<Entry<Integer, String>> expected) {
        try (var stream = index.getStream(SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertEquals(expected, stream.toList());
        }
    }

    private static MemDirectory copyDirectoryWithoutLocks(
            final MemDirectory source) {
        final MemDirectory target = new MemDirectory();
        copyDirectoryRecursively(source, target);
        return target;
    }

    private static void copyDirectoryRecursively(final MemDirectory source,
            final MemDirectory target) {
        source.getFileNames().forEach(name -> {
            if (name.endsWith(".lock")) {
                return;
            }
            try {
                target.setFileSequence(name, source.getFileSequence(name));
                return;
            } catch (final org.hestiastore.index.IndexException ignored) {
                // The name belongs to a subdirectory, not a file.
            }
            final MemDirectory sourceChild = (MemDirectory) source
                    .openSubDirectory(name);
            final MemDirectory targetChild = (MemDirectory) target
                    .openSubDirectory(name);
            copyDirectoryRecursively(sourceChild, targetChild);
        });
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
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
                    "event=wal_retention_pressure_start");
            assertEquals(1L, warningCount,
                    "Expected only one retention-pressure warning within throttle interval.");
        } finally {
            appender.detach();
        }
    }

    @Test
    void walRetentionPressureEmitsStructuredStartAndClearedEvents() {
        final TestLogAppender appender = TestLogAppender
                .attachRootAppender(Level.INFO);
        try {
            final MemDirectory directory = new MemDirectory();
            final Wal wal = Wal.builder()//
                    .withSegmentSizeBytes(96L)//
                    .withMaxBytesBeforeForcedCheckpoint(192L)//
                    .build();
            final IndexConfiguration<String, String> conf = IndexConfiguration
                    .<String, String>builder()//
                    .withKeyClass(String.class)//
                    .withValueClass(String.class)//
                    .withName("wal-retention-pressure-structured-events-it")//
                    .withWal(wal)//
                    .build();

            try (SegmentIndex<String, String> index = SegmentIndex
                    .create(directory, conf)) {
                for (int i = 0; i < 300; i++) {
                    index.put("structured-" + i, "value-" + i);
                }
            }

            assertTrue(appender.countMessageContaining(
                    "event=wal_retention_pressure_start") >= 1L);
            assertTrue(appender.countMessageContaining(
                    "event=wal_retention_pressure_cleared") >= 1L);
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
            return attachRootAppender(Level.WARN);
        }

        static TestLogAppender attachRootAppender(final Level level) {
            final LoggerContext context = (LoggerContext) LogManager
                    .getContext(false);
            final Configuration configuration = context.getConfiguration();
            final String name = "wal-retention-pressure-test-appender-"
                    + System.nanoTime();
            final TestLogAppender appender = new TestLogAppender(name, context);
            appender.start();
            configuration.getRootLogger().addAppender(appender, level, null);
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
