package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalBuilder;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.junit.jupiter.api.Test;

class WalRuntimeTest {

    private static final TypeDescriptorString STRING_DESCRIPTOR = new TypeDescriptorString();

    @Test
    void recoverTruncatesInvalidTailAndReplaysValidPrefix() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withSegmentSizeBytes(1024L)//
                .build();
        final long lsn1;
        final long lsn2;
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            lsn1 = runtime.appendPut("k1", "v1");
            lsn2 = runtime.appendDelete("k2");
        }
        appendGarbageTail(root);

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertTrue(result.truncatedTail());
            assertEquals(lsn2, result.maxLsn());
        }

        assertEquals(2, replayed.size());
        replayed.sort(Comparator.comparingLong(WalRuntime.ReplayRecord::getLsn));
        assertEquals(lsn1, replayed.get(0).getLsn());
        assertEquals(WalRuntime.Operation.PUT,
                replayed.get(0).getOperation());
        assertEquals("k1", replayed.get(0).getKey());
        assertEquals("v1", replayed.get(0).getValue());
        assertEquals(lsn2, replayed.get(1).getLsn());
        assertEquals(WalRuntime.Operation.DELETE,
                replayed.get(1).getOperation());
        assertEquals("k2", replayed.get(1).getKey());
    }

    @Test
    void recoverFailFastThrowsWhenTailIsCorrupted() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)//
                .withSegmentSizeBytes(1024L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        appendGarbageTail(root);

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(r -> {
                    }));
        }
    }

    @Test
    void recoverTruncatesPartiallyWrittenLastRecord() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withSegmentSizeBytes(4096L)//
                .build();
        final long firstLsn;
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            firstLsn = runtime.appendPut("k1", "v1");
            runtime.appendPut("k2", "v2");
        }
        truncateWalTailBy(root, 1);

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertTrue(result.truncatedTail());
            assertEquals(firstLsn, result.maxLsn());
        }

        assertEquals(1, replayed.size());
        assertEquals(firstLsn, replayed.get(0).getLsn());
        assertEquals("k1", replayed.get(0).getKey());
    }

    @Test
    void recoverDetectsInvalidOperationCodeEvenWithValidCrc() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withSegmentSizeBytes(4096L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        rewriteFirstRecordBody(root, body -> {
            body[12] = (byte) 99;
        });

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(ignoredRecord -> {
                    });
            assertTrue(result.truncatedTail());
            assertEquals(0L, result.maxLsn());
        }
    }

    @Test
    void recoverFailFastOnInvalidOperationCodeWhenConfigured() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)//
                .withSegmentSizeBytes(4096L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        rewriteFirstRecordBody(root, body -> {
            body[12] = (byte) 99;
        });

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverRepairIsIdempotentWithinSameRuntime() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withSegmentSizeBytes(1024L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
            runtime.appendPut("k2", "v2");
        }
        appendGarbageTail(root);

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final List<WalRuntime.ReplayRecord<String, String>> firstReplay = new ArrayList<>();
            final WalRuntime.RecoveryResult first = runtime
                    .recover(firstReplay::add);
            assertTrue(first.truncatedTail());
            assertEquals(2L, first.maxLsn());

            final List<WalRuntime.ReplayRecord<String, String>> secondReplay = new ArrayList<>();
            final WalRuntime.RecoveryResult second = runtime
                    .recover(secondReplay::add);
            assertFalse(second.truncatedTail());
            assertEquals(first.maxLsn(), second.maxLsn());
            assertEquals(first.lastReplayedLsn(), second.lastReplayedLsn());
            assertEquals(firstReplay.size(), secondReplay.size());
            for (int i = 0; i < firstReplay.size(); i++) {
                assertEquals(firstReplay.get(i).getLsn(),
                        secondReplay.get(i).getLsn());
                assertEquals(firstReplay.get(i).getOperation(),
                        secondReplay.get(i).getOperation());
                assertEquals(firstReplay.get(i).getKey(),
                        secondReplay.get(i).getKey());
                assertEquals(firstReplay.get(i).getValue(),
                        secondReplay.get(i).getValue());
            }
        }
    }

    @Test
    void checkpointDeletesSealedSegmentsAfterRotation() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withSegmentSizeBytes(96L)//
                .build();
        long lastLsn = 0L;
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            for (int i = 0; i < 30; i++) {
                lastLsn = runtime.appendPut("k-" + i, "v-" + i);
            }
            assertTrue(countWalSegments(root) > 1);
            runtime.onCheckpoint(lastLsn);
            assertTrue(countWalSegments(root) <= 1);
        }
    }

    @Test
    void checkpointCleanupIsIdempotentForRepeatedCheckpointLsn() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withSegmentSizeBytes(96L)//
                .build();
        long checkpointLsn = 0L;
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            for (int i = 0; i < 30; i++) {
                checkpointLsn = runtime.appendPut("idem-k-" + i, "idem-v-" + i);
            }
            assertTrue(runtime.statsSnapshot().segmentCount() > 1);

            runtime.onCheckpoint(checkpointLsn);
            final WalStats afterFirst = runtime.statsSnapshot();
            assertTrue(afterFirst.segmentCount() <= 1);

            runtime.onCheckpoint(checkpointLsn);
            runtime.onCheckpoint(checkpointLsn - 1L);
            final WalStats afterRepeat = runtime.statsSnapshot();

            assertEquals(afterFirst.segmentCount(), afterRepeat.segmentCount());
            assertEquals(afterFirst.retainedBytes(), afterRepeat.retainedBytes());
            assertEquals(afterFirst.checkpointLsn(), afterRepeat.checkpointLsn());
            assertFalse(runtime.isRetentionPressure());
        }
    }

    @Test
    void checkpointCleanupLogsAreThrottled() {
        final TestLogAppender appender = TestLogAppender.attachToLogger(
                WalRuntime.class.getName(), Level.INFO);
        try {
            final MemDirectory root = new MemDirectory();
            final Wal wal = Wal.builder()//
                    .withSegmentSizeBytes(96L)//
                    .build();
            try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                    STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
                for (int i = 0; i < 200; i++) {
                    final long lsn = runtime.appendPut("log-k-" + i,
                            "log-v-" + i);
                    runtime.onCheckpoint(lsn);
                }
            }
            final long cleanupLogCount = appender
                    .countMessageContaining("event=wal_checkpoint_cleanup");
            assertTrue(cleanupLogCount >= 1L);
            assertTrue(cleanupLogCount <= 2L,
                    "Expected throttled wal_checkpoint_cleanup logs, actual count="
                            + cleanupLogCount);
        } finally {
            appender.detach();
        }
    }

    @Test
    void retentionPressureClearsAfterCheckpointCleanup() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withSegmentSizeBytes(96L)//
                .withMaxBytesBeforeForcedCheckpoint(200L)//
                .build();
        long lastLsn = 0L;
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            for (int i = 0; i < 40; i++) {
                lastLsn = runtime.appendPut("ret-" + i, "val-" + i);
            }
            assertTrue(runtime.isRetentionPressure());
            runtime.onCheckpoint(lastLsn);
            assertFalse(runtime.isRetentionPressure());
        }
    }

    @Test
    void retentionPressureIgnoredWhenOnlyActiveSegmentExceedsLimit() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withSegmentSizeBytes(4096L)//
                .withMaxBytesBeforeForcedCheckpoint(1L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("single-segment-key", "single-segment-value");
            assertFalse(runtime.isRetentionPressure());
        }
    }

    @Test
    void openCreatesFormatMarker() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> ignored = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final Directory walDirectory = root.openSubDirectory("wal");
            assertTrue(walDirectory.isFileExists("format.meta"));
        }
    }

    @Test
    void openFailsWhenFormatMarkerTempSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> "format.meta.tmp".equals(name), attempt -> false);

        assertThrows(IndexException.class, () -> WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void openFailsWhenFormatMarkerSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> "format.meta".equals(name), attempt -> false);

        assertThrows(IndexException.class, () -> WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void openFailsWhenFormatTempCleanupMetadataSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> ignored = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            // format.meta created
        }
        storage.overwrite("format.meta.tmp",
                "version=999\nchecksum=1\n"
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                0, "version=999\nchecksum=1\n".length());
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> false, attempt -> attempt == 1);

        assertThrows(IndexException.class, () -> WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void openKeepsFormatMetadataWhenTempPromotionSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final byte[] payload = "version=1\n"
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final int checksum = WalRuntime.computeCrc32(payload, 0, payload.length);
        final byte[] formatMeta = ("version=1\nchecksum=" + checksum + "\n")
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        storage.overwrite("format.meta.tmp", formatMeta, 0, formatMeta.length);
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> "format.meta".equals(name), attempt -> false);

        try (WalRuntime<String, String> ignored = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertTrue(storage.exists("format.meta"));
            assertFalse(storage.exists("format.meta.tmp"));
        }
    }

    @Test
    void openFailsWhenFormatMarkerIsCorrupted() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter("format.meta", Directory.Access.OVERWRITE)) {
            writer.write("version=999\nchecksum=1\n"
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }
        final Wal wal = Wal.builder().build();

        assertThrows(IndexException.class, () -> WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void openFailsWhenFormatMarkerHasNonNumericChecksum() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter("format.meta", Directory.Access.OVERWRITE)) {
            writer.write("version=1\nchecksum=NaN\n"
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }
        final Wal wal = Wal.builder().build();

        assertThrows(IndexException.class, () -> WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void openPromotesValidFormatTempMarkerWhenMainMissing() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        final byte[] payload = "version=1\n"
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final int checksum = WalRuntime.computeCrc32(payload, 0, payload.length);
        writeWalMetadata(root, "format.meta.tmp",
                "version=1\nchecksum=" + checksum + "\n");
        assertFalse(walDirectory.isFileExists("format.meta"));

        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> ignored = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertTrue(walDirectory.isFileExists("format.meta"));
            assertFalse(walDirectory.isFileExists("format.meta.tmp"));
        }
    }

    @Test
    void openDropsStaleFormatTempMarkerWhenMainExists() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        final byte[] payload = "version=1\n"
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final int checksum = WalRuntime.computeCrc32(payload, 0, payload.length);
        writeWalMetadata(root, "format.meta", "version=1\nchecksum=" + checksum
                + "\n");
        writeWalMetadata(root, "format.meta.tmp", "version=999\nchecksum=1\n");

        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> ignored = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertTrue(walDirectory.isFileExists("format.meta"));
            assertFalse(walDirectory.isFileExists("format.meta.tmp"));
        }
    }

    @Test
    void recoverPromotesValidCheckpointTempMetadataWhenMainMissing() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        final long lsn;
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            lsn = runtime.appendPut("k1", "v1");
        }
        final Directory walDirectory = root.openSubDirectory("wal");
        walDirectory.deleteFile("checkpoint.meta");
        writeWalMetadata(root, "checkpoint.meta.tmp", String.valueOf(lsn));
        assertFalse(walDirectory.isFileExists("checkpoint.meta"));

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(ignoredRecord -> {
                    });
            assertEquals(lsn, result.maxLsn());
            assertEquals(lsn, runtime.statsSnapshot().checkpointLsn());
            assertTrue(walDirectory.isFileExists("checkpoint.meta"));
            assertFalse(walDirectory.isFileExists("checkpoint.meta.tmp"));
            assertEquals(lsn, readCheckpointMetadataLsn(root));
        }
    }

    @Test
    void recoverDropsInvalidCheckpointTempMetadataWhenMainMissing() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final Directory walDirectory = root.openSubDirectory("wal");
        walDirectory.deleteFile("checkpoint.meta");
        writeWalMetadata(root, "checkpoint.meta.tmp", "invalid-checkpoint");

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertEquals(1L, result.maxLsn());
            assertEquals(0L, runtime.statsSnapshot().checkpointLsn());
            assertFalse(walDirectory.isFileExists("checkpoint.meta.tmp"));
        }
        assertEquals(1, replayed.size());
        assertEquals(1L, replayed.get(0).getLsn());
    }

    @Test
    void recoverIgnoresNonWalFilesInWalDirectory() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
            runtime.appendPut("k2", "v2");
        }
        final Directory walDirectory = root.openSubDirectory("wal");
        walDirectory.touch("notes.txt");
        walDirectory.touch("00000000000000000001.wal.bak");
        walDirectory.touch("checkpoint.meta.tmp");
        walDirectory.mkdir("archived");

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertFalse(result.truncatedTail());
            assertEquals(2L, result.maxLsn());
        }
        replayed.sort(Comparator.comparingLong(WalRuntime.ReplayRecord::getLsn));
        assertEquals(2, replayed.size());
        assertEquals(1L, replayed.get(0).getLsn());
        assertEquals(2L, replayed.get(1).getLsn());
    }

    @Test
    void recoverOrdersSegmentsByNumericBaseLsn() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
            runtime.appendPut("k2", "v2");
        }

        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        final String originalSegment = singleWalSegmentName(walDirectory);
        final byte[] segmentBytes = walDirectory.getFileSequence(originalSegment)
                .toByteArrayCopy();
        final int firstBodyLength = WalRuntime.readInt(segmentBytes, 0);
        final int firstRecordLength = 4 + firstBodyLength;
        final int secondBodyLength = WalRuntime.readInt(segmentBytes,
                firstRecordLength);
        final int secondRecordLength = 4 + secondBodyLength;
        assertTrue(firstRecordLength + secondRecordLength <= segmentBytes.length);
        walDirectory.setFileSequence("00000000000000000002.wal", ByteSequences
                .wrap(Arrays.copyOfRange(segmentBytes, 0, firstRecordLength)));
        walDirectory.setFileSequence("00000000000000000010.wal",
                ByteSequences.wrap(Arrays.copyOfRange(segmentBytes,
                        firstRecordLength,
                        firstRecordLength + secondRecordLength)));
        walDirectory.deleteFile(originalSegment);

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertFalse(result.truncatedTail());
            assertEquals(2L, result.maxLsn());
        }
        replayed.sort(Comparator.comparingLong(WalRuntime.ReplayRecord::getLsn));
        assertEquals(2, replayed.size());
        assertEquals(1L, replayed.get(0).getLsn());
        assertEquals(2L, replayed.get(1).getLsn());
    }

    @Test
    void recoverRejectsInvalidWalSegmentName() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        walDirectory.touch("invalid-segment.wal");
        final Wal wal = Wal.builder().build();

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverRejectsWalSegmentDirectoryEntry() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        walDirectory.mkdir("00000000000000000001.wal");
        final Wal wal = Wal.builder().build();

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverRejectsDuplicateWalSegmentBaseLsnEntries() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final String duplicatedSegment = walSegmentNames(storage).stream()
                .findFirst().orElseThrow();
        final WalStorage duplicateListing = new DuplicateListingStorage(storage,
                duplicatedSegment);

        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                duplicateListing, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final IndexException exception = assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
            assertTrue(exception.getMessage()
                    .contains("Duplicate WAL segment base LSN"));
        }
    }

    @Test
    void recoverClampsCheckpointMetadataAndCapsLastReplayedLsn() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()
                .withSegmentSizeBytes(4096L).build();
        final long firstLsn;
        final long secondLsn;
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            firstLsn = runtime.appendPut("k1", "v1");
            secondLsn = runtime.appendPut("k2", "v2");
            runtime.onCheckpoint(secondLsn);
        }

        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        final String segmentName = singleWalSegmentName(walDirectory);
        final byte[] bytes = walDirectory.getFileSequence(segmentName)
                .toByteArrayCopy();
        final int firstRecordLength = 4 + WalRuntime.readInt(bytes, 0);
        walDirectory.setFileSequence(segmentName,
                ByteSequences.wrap(Arrays.copyOf(bytes, firstRecordLength)));

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(ignoredRecord -> {
                    });
            assertFalse(result.truncatedTail());
            assertEquals(firstLsn, result.maxLsn());
            assertEquals(firstLsn, result.lastReplayedLsn());
            assertEquals(firstLsn, runtime.statsSnapshot().checkpointLsn());
        }
        assertEquals(firstLsn, readCheckpointMetadataLsn(root));

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(ignoredRecord -> {
                    });
            assertEquals(firstLsn, result.maxLsn());
            assertEquals(firstLsn, result.lastReplayedLsn());
            assertEquals(firstLsn, runtime.statsSnapshot().checkpointLsn());
        }
    }

    @Test
    void recoverFailsWhenCheckpointMetadataIsNonNumeric() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        writeCheckpointMetadata(root, "not-a-number");

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverFailsWhenCheckpointMetadataIsNegative() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        writeCheckpointMetadata(root, "-1");

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverFailsWhenCheckpointMetadataChecksumIsInvalid() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
            runtime.onCheckpoint(1L);
        }
        writeCheckpointMetadata(root, "lsn=1\nchecksum=0\n");

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverFailsWhenStorageReadFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }

        final WalStorage failingStorage = new FailingStorage(storage,
                name -> name.endsWith(".wal"), name -> false,
                (source, target) -> false);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverFailsWhenCheckpointClampCheckpointTempSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final byte[] staleCheckpoint = "5"
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        storage.overwrite("checkpoint.meta", staleCheckpoint, 0,
                staleCheckpoint.length);
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> "checkpoint.meta.tmp".equals(name), attempt -> false);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverKeepsCheckpointMetadataWhenTempPromotionSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final long lsn = runtime.appendPut("k1", "v1");
            runtime.onCheckpoint(lsn);
        }
        final byte[] checkpoint = storage.readAll("checkpoint.meta");
        storage.overwrite("checkpoint.meta.tmp", checkpoint, 0,
                checkpoint.length);
        storage.delete("checkpoint.meta");
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> "checkpoint.meta".equals(name), attempt -> false);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(ignoredRecord -> {
                    });
            assertEquals(1L, result.maxLsn());
            assertEquals(1L, runtime.statsSnapshot().checkpointLsn());
        }
        assertTrue(storage.exists("checkpoint.meta"));
        assertFalse(storage.exists("checkpoint.meta.tmp"));
    }

    @Test
    void checkpointFailsWhenStorageRenameFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false,
                (source, target) -> "checkpoint.meta.tmp".equals(source)
                        && "checkpoint.meta".equals(target));
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final long lsn = runtime.appendPut("k1", "v1");
            assertThrows(IndexException.class, () -> runtime.onCheckpoint(lsn));
        }
    }

    @Test
    void checkpointFailsWhenCheckpointTempSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> "checkpoint.meta.tmp".equals(name), attempt -> false);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class, () -> runtime.onCheckpoint(1L));
        }
    }

    @Test
    void checkpointFailsWhenCheckpointFileSyncFails() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> "checkpoint.meta".equals(name), attempt -> false);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class, () -> runtime.onCheckpoint(1L));
        }
    }

    @Test
    void checkpointFailsWhenMetadataSyncFailsAfterRename() {
        final Wal wal = Wal.builder().build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> false, attempt -> attempt == 1);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class, () -> runtime.onCheckpoint(1L));
        }
    }

    @Test
    void recoverFailsWhenTailTruncateSyncFails() {
        final Wal wal = Wal.builder()
                .withCorruptionPolicy(WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)
                .build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final String segmentName = walSegmentNames(storage).get(0);
        storage.append(segmentName, new byte[] { 0x01, 0x02, 0x03, 0x04 }, 0,
                4);
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> segmentName.equals(name), attempt -> false);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void recoverFailsWhenTailDeleteMetadataSyncFails() {
        final Wal wal = Wal.builder()
                .withCorruptionPolicy(WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)
                .build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        final String segmentName = walSegmentNames(storage).get(0);
        storage.overwrite(segmentName, new byte[] { 0x01, 0x02, 0x03, 0x04 }, 0,
                4);
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> false, attempt -> attempt == 1);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
        }
    }

    @Test
    void checkpointCleanupFailsWhenMetadataSyncFailsAfterDelete() {
        final Wal wal = Wal.builder()
                .withSegmentSizeBytes(96L).build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        long lastLsn = 0L;
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (runtime.statsSnapshot().segmentCount() < 3 && i < 256) {
                lastLsn = runtime.appendPut("k-" + i, "v-" + i);
                i++;
            }
            assertTrue(runtime.statsSnapshot().segmentCount() >= 3);
        }
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> false, attempt -> attempt == 2);
        final long checkpointLsn = lastLsn;
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.recover(ignoredRecord -> {
            });
            assertThrows(IndexException.class,
                    () -> runtime.onCheckpoint(checkpointLsn));
        }
    }

    @Test
    void checkpointCleanupFailsWhenStorageDeleteFails() {
        final Wal wal = Wal.builder()
                .withSegmentSizeBytes(96L).build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        long lastLsn = 0L;
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (runtime.statsSnapshot().segmentCount() < 3 && i < 256) {
                lastLsn = runtime.appendPut("k-" + i, "v-" + i);
                i++;
            }
            assertTrue(runtime.statsSnapshot().segmentCount() >= 3);
        }
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> name.endsWith(".wal"),
                (source, target) -> false);
        final long checkpointLsn = lastLsn;
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.recover(ignoredRecord -> {
            });
            assertThrows(IndexException.class,
                    () -> runtime.onCheckpoint(checkpointLsn));
        }
    }

    @Test
    void recoverRejectsNonCanonicalSegmentName() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        walDirectory.touch("1.wal");
        final Wal wal = Wal.builder().build();

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final IndexException ex = assertThrows(IndexException.class,
                    () -> runtime.recover(ignoredRecord -> {
                    }));
            assertTrue(ex.getMessage() != null
                    && ex.getMessage().contains("Invalid WAL segment name"));
        }
    }

    @Test
    void recoverDeletesStaleSegmentsAfterCorruptionAndPersistsRepair() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withSegmentSizeBytes(96L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (countWalSegments(root) < 3 && i < 256) {
                runtime.appendPut("k-" + i, "v-" + i);
                i++;
            }
        }
        final List<String> beforeCorruption = walSegmentNames(root);
        assertTrue(beforeCorruption.size() >= 3);
        final String corruptedSegment = beforeCorruption.get(1);
        final Set<String> expectedDeleted = new HashSet<>(
                beforeCorruption.subList(2, beforeCorruption.size()));
        appendGarbageTail(root, corruptedSegment);

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(ignoredRecord -> {
                    });
            assertTrue(result.truncatedTail());
        }
        final Set<String> afterFirstRecovery = new HashSet<>(
                walSegmentNames(root));
        for (final String deleted : expectedDeleted) {
            assertFalse(afterFirstRecovery.contains(deleted));
        }

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(record -> {
                    });
            assertFalse(result.truncatedTail());
        }
        assertEquals(afterFirstRecovery, new HashSet<>(walSegmentNames(root)));
    }

    @Test
    void recoverFailsWhenDroppingNewerSegmentsMetadataSyncFails() {
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withSegmentSizeBytes(96L)//
                .build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (walSegmentNames(storage).size() < 3 && i < 256) {
                runtime.appendPut("k-" + i, "v-" + i);
                i++;
            }
        }
        final List<String> segments = walSegmentNames(storage);
        assertTrue(segments.size() >= 3);
        storage.append(segments.get(1), new byte[] { 0x01, 0x02, 0x03, 0x04 }, 0,
                4);
        final WalStorage failingStorage = new FailingStorage(storage,
                name -> false, name -> false, (source, target) -> false,
                name -> false, attempt -> attempt == 1);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(record -> {
                    }));
        }
    }

    @Test
    void recoverPersistsDeletedSegmentsAcrossCrashAfterRepair() {
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withDurabilityMode(WalDurabilityMode.SYNC)//
                .withSegmentSizeBytes(96L)//
                .build();
        CrashSimulatingStorage storage = new CrashSimulatingStorage();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (walSegmentNames(storage).size() < 3 && i < 256) {
                runtime.appendPut("k-" + i, "v-" + i);
                i++;
            }
        }
        final List<String> beforeCorruption = walSegmentNames(storage);
        assertTrue(beforeCorruption.size() >= 3);
        final String corruptedSegment = beforeCorruption.get(1);
        final Set<String> expectedDeleted = new HashSet<>(
                beforeCorruption.subList(2, beforeCorruption.size()));
        storage.append(corruptedSegment, new byte[] { 0x01, 0x02, 0x03, 0x04 },
                0, 4);
        storage.sync(corruptedSegment);

        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(record -> {
                    });
            assertTrue(result.truncatedTail());
        }

        storage = storage.crashRecover();
        final List<String> segmentsAfterCrash = walSegmentNames(storage);
        for (final String deleted : expectedDeleted) {
            assertFalse(segmentsAfterCrash.contains(deleted),
                    "Deleted segment resurrected after crash: " + deleted);
        }

        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(record -> {
                    });
            assertFalse(result.truncatedTail());
        }
    }

    @Test
    void recoverTruncatesWhenSegmentLsnRegressesAcrossBoundary() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(
                        WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)//
                .withSegmentSizeBytes(96L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (countWalSegments(root) < 3 && i < 256) {
                runtime.appendPut("k-" + i, "v-" + i);
                i++;
            }
        }
        final List<String> segmentNames = walSegmentNames(root);
        assertTrue(segmentNames.size() >= 2);
        final String firstSegment = segmentNames.get(0);
        final String secondSegment = segmentNames.get(1);
        final long firstSegmentMaxLsn = maxLsnInSegment(root, firstSegment);
        assertTrue(firstSegmentMaxLsn > 0L);
        rewriteFirstRecordLsn(root, secondSegment,
                Math.max(1L, firstSegmentMaxLsn - 1L));

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertTrue(result.truncatedTail());
            assertEquals(firstSegmentMaxLsn, result.maxLsn());
        }

        assertEquals(List.of(firstSegment), walSegmentNames(root));
        final long replayedMaxLsn = replayed.stream()
                .mapToLong(WalRuntime.ReplayRecord::getLsn).max().orElse(0L);
        assertEquals(firstSegmentMaxLsn, replayedMaxLsn);
    }

    @Test
    void recoverFailFastWhenSegmentLsnRegressesAcrossBoundary() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)//
                .withSegmentSizeBytes(96L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (countWalSegments(root) < 3 && i < 256) {
                runtime.appendPut("k-" + i, "v-" + i);
                i++;
            }
        }
        final List<String> before = walSegmentNames(root);
        assertTrue(before.size() >= 2);
        final long firstSegmentMaxLsn = maxLsnInSegment(root, before.get(0));
        rewriteFirstRecordLsn(root, before.get(1),
                Math.max(1L, firstSegmentMaxLsn - 1L));

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(record -> {
                    }));
        }
        assertEquals(before, walSegmentNames(root));
    }

    @Test
    void recoverFailFastDoesNotMutateWalFilesAcrossAttempts() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)//
                .withSegmentSizeBytes(96L)//
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            int i = 0;
            while (countWalSegments(root) < 3 && i < 256) {
                runtime.appendPut("ff-k-" + i, "ff-v-" + i);
                i++;
            }
        }
        final List<String> segments = walSegmentNames(root);
        assertTrue(segments.size() >= 3);
        appendGarbageTail(root, segments.get(1));
        final Map<String, byte[]> expectedSnapshot = walSegmentSnapshot(root);

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(record -> {
                    }));
        }
        assertWalSnapshotsEqual(expectedSnapshot, walSegmentSnapshot(root));

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(record -> {
                    }));
        }
        assertWalSnapshotsEqual(expectedSnapshot, walSegmentSnapshot(root));
    }

    @Test
    void syncFailureFailsCurrentAndFollowingWrites() {
        final Wal wal = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC).build();
        final WalStorage failingStorage = new SyncObservingStorage(
                new WalStorageMem(new MemDirectory()), true);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.appendPut("k1", "v1"));
            assertThrows(IndexException.class,
                    () -> runtime.appendPut("k2", "v2"));
        }
    }

    @Test
    void syncFailureEmitsStructuredLogEvent() {
        final TestLogAppender appender = TestLogAppender.attachToLogger(
                WalRuntime.class.getName(), Level.ERROR);
        try {
            final Wal wal = Wal.builder()
                    .withDurabilityMode(WalDurabilityMode.SYNC).build();
            final WalStorage failingStorage = new SyncObservingStorage(
                    new WalStorageMem(new MemDirectory()), true);
            try (WalRuntime<String, String> runtime = WalRuntime.openForTests(
                    wal, failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
                assertThrows(IndexException.class,
                        () -> runtime.appendPut("k1", "v1"));
            }
            assertTrue(appender.countMessageContaining("event=wal_sync_failure")
                    >= 1L);
        } finally {
            appender.detach();
        }
    }

    @Test
    void asyncModeMayLoseUnsyncedWritesAfterCrash() {
        final Wal wal = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.ASYNC).build();
        CrashSimulatingStorage storage = new CrashSimulatingStorage();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }

        storage = storage.crashRecover();
        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertEquals(0L, result.maxLsn());
        }
        assertTrue(replayed.isEmpty());
    }

    @Test
    void syncModePersistsAcknowledgedWritesAfterCrash() {
        final Wal wal = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC).build();
        CrashSimulatingStorage storage = new CrashSimulatingStorage();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }

        storage = storage.crashRecover();
        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertEquals(1L, result.maxLsn());
        }
        assertEquals(1, replayed.size());
        assertEquals("k1", replayed.get(0).getKey());
        assertEquals("v1", replayed.get(0).getValue());
    }

    @Test
    void groupSyncModePersistsAcknowledgedWritesAfterCrash() {
        final Wal wal = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.GROUP_SYNC)
                .withGroupSyncDelayMillis(1).build();
        CrashSimulatingStorage storage = new CrashSimulatingStorage();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }

        storage = storage.crashRecover();
        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult result = runtime
                    .recover(replayed::add);
            assertEquals(1L, result.maxLsn());
        }
        assertEquals(1, replayed.size());
        assertEquals("k1", replayed.get(0).getKey());
        assertEquals("v1", replayed.get(0).getValue());
    }

    @Test
    void randomCrashRecoveryCyclesRemainDeterministicForDurableModes() {
        runRandomCrashRecoveryHarness(WalDurabilityMode.SYNC, 17L);
        runRandomCrashRecoveryHarness(WalDurabilityMode.GROUP_SYNC, 19L);
    }

    @Test
    void groupSyncSyncsAllPendingRotatedSegments() throws Exception {
        final Wal wal = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.GROUP_SYNC)
                .withGroupSyncDelayMillis(500)
                .withGroupSyncMaxBatchBytes(16 * 1024 * 1024)
                .withSegmentSizeBytes(96L).build();
        final SyncObservingStorage storage = new SyncObservingStorage(
                new WalStorageMem(new MemDirectory()), false);
        final int writers = 24;
        final ExecutorService executor = Executors.newFixedThreadPool(writers);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readyLatch = new CountDownLatch(writers);
        final List<Future<Long>> writes = new ArrayList<>(writers);
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            for (int i = 0; i < writers; i++) {
                final int value = i;
                writes.add(executor.submit(() -> {
                    readyLatch.countDown();
                    startLatch.await();
                    return runtime.appendPut("k-" + value,
                            "v-" + value + "-payload");
                }));
            }
            assertTrue(readyLatch.await(5, TimeUnit.SECONDS));
            startLatch.countDown();
            for (final Future<Long> write : writes) {
                write.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
        final Set<String> walFiles = storage.listFileNames()
                .filter(name -> name.endsWith(".wal"))
                .collect(Collectors.toSet());
        assertTrue(walFiles.size() > 1);
        assertTrue(storage.syncedWalSegments().containsAll(walFiles),
                "Expected synced WAL segments " + walFiles + " but got "
                        + storage.syncedWalSegments());
    }

    private static int countWalSegments(final MemDirectory root) {
        final Directory walDirectory = root.openSubDirectory("wal");
        return (int) walDirectory.getFileNames().filter(
                name -> name.endsWith(".wal")).count();
    }

    private static void appendGarbageTail(final MemDirectory root) {
        final Directory walDirectory = root.openSubDirectory("wal");
        final String segmentName = walDirectory.getFileNames()
                .filter(name -> name.endsWith(".wal")).findFirst()
                .orElseThrow();
        appendGarbageTail(root, segmentName);
    }

    private static void appendGarbageTail(final MemDirectory root,
            final String segmentName) {
        final Directory walDirectory = root.openSubDirectory("wal");
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter(segmentName, Directory.Access.APPEND)) {
            writer.write(new byte[] { 0x01, 0x02, 0x03, 0x04 });
        }
    }

    private static List<String> walSegmentNames(final MemDirectory root) {
        final Directory walDirectory = root.openSubDirectory("wal");
        return walDirectory.getFileNames().filter(name -> name.endsWith(".wal"))
                .sorted().toList();
    }

    private static List<String> walSegmentNames(final WalStorage storage) {
        try (java.util.stream.Stream<String> names = storage.listFileNames()) {
            return names.filter(name -> name.endsWith(".wal")).sorted().toList();
        }
    }

    private static Map<String, byte[]> walSegmentSnapshot(final MemDirectory root) {
        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        final Map<String, byte[]> snapshot = new LinkedHashMap<>();
        for (final String name : walSegmentNames(root)) {
            snapshot.put(name, walDirectory.getFileSequence(name).toByteArrayCopy());
        }
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

    private static void truncateWalTailBy(final MemDirectory root,
            final int bytesToTrim) {
        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        final String segmentName = singleWalSegmentName(walDirectory);
        final byte[] bytes = walDirectory.getFileSequence(segmentName)
                .toByteArrayCopy();
        final int newLength = Math.max(0, bytes.length - bytesToTrim);
        walDirectory.setFileSequence(segmentName,
                ByteSequences.wrap(Arrays.copyOf(bytes, newLength)));
    }

    private static void rewriteFirstRecordBody(final MemDirectory root,
            final java.util.function.Consumer<byte[]> bodyMutator) {
        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        final String segmentName = singleWalSegmentName(walDirectory);
        rewriteFirstRecordBody(walDirectory, segmentName, bodyMutator);
    }

    private static void rewriteFirstRecordLsn(final MemDirectory root,
            final String segmentName, final long newLsn) {
        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        rewriteFirstRecordBody(walDirectory, segmentName, body -> {
            WalRuntime.putLong(body, 4, newLsn);
        });
    }

    private static void rewriteFirstRecordBody(final MemDirectory walDirectory,
            final String segmentName,
            final java.util.function.Consumer<byte[]> bodyMutator) {
        final ByteSequence sequence = walDirectory.getFileSequence(segmentName);
        final byte[] segmentBytes = sequence.toByteArrayCopy();
        final int bodyLength = WalRuntime.readInt(segmentBytes, 0);
        final byte[] body = Arrays.copyOfRange(segmentBytes, 4,
                4 + bodyLength);
        bodyMutator.accept(body);
        final int updatedCrc = WalRuntime.computeCrc32(body, 4,
                body.length - 4);
        WalRuntime.putInt(body, 0, updatedCrc);
        System.arraycopy(body, 0, segmentBytes, 4, body.length);
        walDirectory.setFileSequence(segmentName,
                ByteSequences.wrap(segmentBytes));
    }

    private static long maxLsnInSegment(final MemDirectory root,
            final String segmentName) {
        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        final byte[] bytes = walDirectory.getFileSequence(segmentName)
                .toByteArrayCopy();
        long offset = 0L;
        long maxLsn = 0L;
        while (offset + 4L <= bytes.length) {
            final int bodyLength = WalRuntime.readInt(bytes, (int) offset);
            if (bodyLength <= 0 || offset + 4L + bodyLength > bytes.length) {
                break;
            }
            final long lsn = WalRuntime.readLong(bytes, (int) offset + 8);
            if (lsn > maxLsn) {
                maxLsn = lsn;
            }
            offset += 4L + bodyLength;
        }
        return maxLsn;
    }

    private static String singleWalSegmentName(final MemDirectory walDirectory) {
        return walDirectory.getFileNames().filter(name -> name.endsWith(".wal"))
                .findFirst().orElseThrow();
    }

    private static void writeCheckpointMetadata(final MemDirectory root,
            final String value) {
        final Directory walDirectory = root.openSubDirectory("wal");
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter("checkpoint.meta", Directory.Access.OVERWRITE)) {
            writer.write(value.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }
    }

    private static long readCheckpointMetadataLsn(final MemDirectory root) {
        final MemDirectory walDirectory = (MemDirectory) root
                .openSubDirectory("wal");
        if (!walDirectory.isFileExists("checkpoint.meta")) {
            return 0L;
        }
        final String text = new String(
                walDirectory.getFileSequence("checkpoint.meta").toByteArrayCopy(),
                java.nio.charset.StandardCharsets.US_ASCII).trim();
        if (text.isEmpty()) {
            return 0L;
        }
        if (!text.contains("=")) {
            return Long.parseLong(text);
        }
        Long lsn = null;
        Integer checksum = null;
        for (final String line : text.split("\\R")) {
            final String[] parts = line.split("=", 2);
            if (parts.length != 2) {
                continue;
            }
            final String key = parts[0].trim();
            final String value = parts[1].trim();
            if ("lsn".equals(key)) {
                lsn = Long.valueOf(value);
            } else if ("checksum".equals(key)) {
                checksum = Integer.valueOf(value);
            }
        }
        if (lsn == null || checksum == null) {
            throw new IllegalStateException("Invalid checkpoint metadata.");
        }
        final byte[] payload = ("lsn=" + lsn + "\n")
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        final int expectedChecksum = WalRuntime.computeCrc32(payload, 0,
                payload.length);
        if (checksum.intValue() != expectedChecksum) {
            throw new IllegalStateException("Invalid checkpoint checksum.");
        }
        return lsn.longValue();
    }

    private static void writeWalMetadata(final MemDirectory root,
            final String fileName, final String value) {
        final Directory walDirectory = root.openSubDirectory("wal");
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter(fileName, Directory.Access.OVERWRITE)) {
            writer.write(value.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }
    }

    private static void runRandomCrashRecoveryHarness(
            final WalDurabilityMode durabilityMode, final long seed) {
        final WalBuilder walBuilder = Wal.builder()
                .withDurabilityMode(durabilityMode).withSegmentSizeBytes(128L);
        if (durabilityMode == WalDurabilityMode.GROUP_SYNC) {
            walBuilder.withGroupSyncDelayMillis(1)
                    .withGroupSyncMaxBatchBytes(256);
        }
        final Wal wal = walBuilder.build();
        final Random random = new Random(seed);
        final int keySpace = 16;
        final int cycles = 14;
        final int operationsPerCycle = 48;
        final Map<String, String> expectedState = new LinkedHashMap<>();
        CrashSimulatingStorage storage = new CrashSimulatingStorage();
        long expectedMaxLsn = 0L;

        for (int cycle = 0; cycle < cycles; cycle++) {
            final Map<String, String> recoveredState = new LinkedHashMap<>();
            try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                    storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
                final WalRuntime.RecoveryResult recovery = runtime
                        .recover(record -> applyReplayRecord(recoveredState,
                                record));
                assertEquals(expectedState, recoveredState);
                assertEquals(expectedMaxLsn, recovery.maxLsn());
                long cycleMaxLsn = expectedMaxLsn;

                for (int i = 0; i < operationsPerCycle; i++) {
                    final String key = "rk-" + random.nextInt(keySpace);
                    final boolean delete = random.nextInt(5) == 0;
                    final long lsn;
                    if (delete) {
                        lsn = runtime.appendDelete(key);
                        expectedState.remove(key);
                    } else {
                        final String value = "rv-" + cycle + "-" + i + "-"
                                + random.nextInt(10_000);
                        lsn = runtime.appendPut(key, value);
                        expectedState.put(key, value);
                    }
                    cycleMaxLsn = Math.max(cycleMaxLsn, lsn);
                }
                expectedMaxLsn = cycleMaxLsn;
            }
            storage = storage.crashRecover();
        }

        final Map<String, String> finalRecovered = new LinkedHashMap<>();
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                storage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final WalRuntime.RecoveryResult recovery = runtime
                    .recover(record -> applyReplayRecord(finalRecovered, record));
            assertEquals(expectedState, finalRecovered);
            assertEquals(expectedMaxLsn, recovery.maxLsn());
        }
    }

    private static void applyReplayRecord(final Map<String, String> state,
            final WalRuntime.ReplayRecord<String, String> record) {
        if (record.getOperation() == WalRuntime.Operation.DELETE) {
            state.remove(record.getKey());
            return;
        }
        state.put(record.getKey(), record.getValue());
    }

    private static final class TestLogAppender extends AbstractAppender {

        private final LoggerContext loggerContext;
        private final String loggerConfigName;
        private final List<LogEvent> events = new ArrayList<>();

        private TestLogAppender(final String name,
                final LoggerContext loggerContext,
                final String loggerConfigName) {
            super(name, null, PatternLayout.createDefaultLayout(), true,
                    Property.EMPTY_ARRAY);
            this.loggerContext = loggerContext;
            this.loggerConfigName = loggerConfigName;
        }

        static TestLogAppender attachToLogger(final String loggerName,
                final Level level) {
            final LoggerContext context = (LoggerContext) LogManager
                    .getContext(false);
            final Configuration configuration = context.getConfiguration();
            final LoggerConfig loggerConfig = configuration
                    .getLoggerConfig(loggerName);
            final String appenderName = "wal-runtime-test-appender-"
                    + System.nanoTime();
            final TestLogAppender appender = new TestLogAppender(appenderName,
                    context, loggerConfig.getName());
            appender.start();
            loggerConfig.addAppender(appender, level, null);
            context.updateLoggers();
            return appender;
        }

        void detach() {
            final Configuration configuration = loggerContext
                    .getConfiguration();
            final LoggerConfig loggerConfig = configuration
                    .getLoggerConfig(loggerConfigName);
            loggerConfig.removeAppender(getName());
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

    private static final class FailingStorage implements WalStorage {

        private final WalStorage delegate;
        private final Predicate<String> failRead;
        private final Predicate<String> failDelete;
        private final BiPredicate<String, String> failRename;
        private final Predicate<String> failSync;
        private final IntPredicate failSyncMetadataAttempt;
        private int syncMetadataAttempts;

        private FailingStorage(final WalStorage delegate,
                final Predicate<String> failRead,
                final Predicate<String> failDelete,
                final BiPredicate<String, String> failRename) {
            this(delegate, failRead, failDelete, failRename, name -> false,
                    attempt -> false);
        }

        private FailingStorage(final WalStorage delegate,
                final Predicate<String> failRead,
                final Predicate<String> failDelete,
                final BiPredicate<String, String> failRename,
                final Predicate<String> failSync,
                final IntPredicate failSyncMetadataAttempt) {
            this.delegate = delegate;
            this.failRead = failRead;
            this.failDelete = failDelete;
            this.failRename = failRename;
            this.failSync = failSync;
            this.failSyncMetadataAttempt = failSyncMetadataAttempt;
            this.syncMetadataAttempts = 0;
        }

        @Override
        public boolean exists(final String fileName) {
            return delegate.exists(fileName);
        }

        @Override
        public void touch(final String fileName) {
            delegate.touch(fileName);
        }

        @Override
        public long size(final String fileName) {
            return delegate.size(fileName);
        }

        @Override
        public void append(final String fileName, final byte[] bytes,
                final int offset, final int length) {
            delegate.append(fileName, bytes, offset, length);
        }

        @Override
        public void overwrite(final String fileName, final byte[] bytes,
                final int offset, final int length) {
            delegate.overwrite(fileName, bytes, offset, length);
        }

        @Override
        public byte[] readAll(final String fileName) {
            return delegate.readAll(fileName);
        }

        @Override
        public int read(final String fileName, final long position,
                final byte[] destination, final int offset, final int length) {
            if (failRead.test(fileName)) {
                throw new IndexException("simulated read failure");
            }
            return delegate.read(fileName, position, destination, offset,
                    length);
        }

        @Override
        public void truncate(final String fileName, final long sizeBytes) {
            delegate.truncate(fileName, sizeBytes);
        }

        @Override
        public boolean delete(final String fileName) {
            if (failDelete.test(fileName)) {
                throw new IndexException("simulated delete failure");
            }
            return delegate.delete(fileName);
        }

        @Override
        public void rename(final String currentFileName,
                final String newFileName) {
            if (failRename.test(currentFileName, newFileName)) {
                throw new IndexException("simulated rename failure");
            }
            delegate.rename(currentFileName, newFileName);
        }

        @Override
        public java.util.stream.Stream<String> listFileNames() {
            return delegate.listFileNames();
        }

        @Override
        public void sync(final String fileName) {
            if (failSync.test(fileName)) {
                throw new IndexException("simulated sync failure");
            }
            delegate.sync(fileName);
        }

        @Override
        public void syncMetadata() {
            syncMetadataAttempts++;
            if (failSyncMetadataAttempt.test(syncMetadataAttempts)) {
                throw new IndexException("simulated metadata sync failure");
            }
            delegate.syncMetadata();
        }
    }

    private static final class DuplicateListingStorage implements WalStorage {

        private final WalStorage delegate;
        private final String duplicateFileName;

        private DuplicateListingStorage(final WalStorage delegate,
                final String duplicateFileName) {
            this.delegate = delegate;
            this.duplicateFileName = duplicateFileName;
        }

        @Override
        public boolean exists(final String fileName) {
            return delegate.exists(fileName);
        }

        @Override
        public void touch(final String fileName) {
            delegate.touch(fileName);
        }

        @Override
        public long size(final String fileName) {
            return delegate.size(fileName);
        }

        @Override
        public void append(final String fileName, final byte[] bytes,
                final int offset, final int length) {
            delegate.append(fileName, bytes, offset, length);
        }

        @Override
        public void overwrite(final String fileName, final byte[] bytes,
                final int offset, final int length) {
            delegate.overwrite(fileName, bytes, offset, length);
        }

        @Override
        public byte[] readAll(final String fileName) {
            return delegate.readAll(fileName);
        }

        @Override
        public int read(final String fileName, final long position,
                final byte[] destination, final int offset, final int length) {
            return delegate.read(fileName, position, destination, offset,
                    length);
        }

        @Override
        public void truncate(final String fileName, final long sizeBytes) {
            delegate.truncate(fileName, sizeBytes);
        }

        @Override
        public boolean delete(final String fileName) {
            return delegate.delete(fileName);
        }

        @Override
        public void rename(final String currentFileName,
                final String newFileName) {
            delegate.rename(currentFileName, newFileName);
        }

        @Override
        public java.util.stream.Stream<String> listFileNames() {
            return java.util.stream.Stream.concat(delegate.listFileNames(),
                    java.util.stream.Stream.of(duplicateFileName));
        }

        @Override
        public void sync(final String fileName) {
            delegate.sync(fileName);
        }

        @Override
        public void syncMetadata() {
            delegate.syncMetadata();
        }
    }

    private static final class SyncObservingStorage implements WalStorage {

        private final WalStorage delegate;
        private final boolean failOnWalSync;
        private final Set<String> syncedWalSegments = new HashSet<>();

        private SyncObservingStorage(final WalStorage delegate,
                final boolean failOnWalSync) {
            this.delegate = delegate;
            this.failOnWalSync = failOnWalSync;
        }

        private Set<String> syncedWalSegments() {
            return Set.copyOf(syncedWalSegments);
        }

        @Override
        public boolean exists(final String fileName) {
            return delegate.exists(fileName);
        }

        @Override
        public void touch(final String fileName) {
            delegate.touch(fileName);
        }

        @Override
        public long size(final String fileName) {
            return delegate.size(fileName);
        }

        @Override
        public void append(final String fileName, final byte[] bytes,
                final int offset, final int length) {
            delegate.append(fileName, bytes, offset, length);
        }

        @Override
        public void overwrite(final String fileName, final byte[] bytes,
                final int offset, final int length) {
            delegate.overwrite(fileName, bytes, offset, length);
        }

        @Override
        public byte[] readAll(final String fileName) {
            return delegate.readAll(fileName);
        }

        @Override
        public int read(final String fileName, final long position,
                final byte[] destination, final int offset, final int length) {
            return delegate.read(fileName, position, destination, offset,
                    length);
        }

        @Override
        public void truncate(final String fileName, final long sizeBytes) {
            delegate.truncate(fileName, sizeBytes);
        }

        @Override
        public boolean delete(final String fileName) {
            return delegate.delete(fileName);
        }

        @Override
        public void rename(final String currentFileName,
                final String newFileName) {
            delegate.rename(currentFileName, newFileName);
        }

        @Override
        public java.util.stream.Stream<String> listFileNames() {
            return delegate.listFileNames();
        }

        @Override
        public void sync(final String fileName) {
            if (fileName != null && fileName.endsWith(".wal")) {
                syncedWalSegments.add(fileName);
                if (failOnWalSync) {
                    throw new IndexException("simulated sync failure");
                }
            }
            delegate.sync(fileName);
        }

        @Override
        public void syncMetadata() {
            delegate.syncMetadata();
        }
    }

    private static final class CrashSimulatingStorage implements WalStorage {

        private final Map<String, byte[]> workingFiles;
        private final Map<String, byte[]> durableFiles;

        private CrashSimulatingStorage() {
            this(new LinkedHashMap<>(), new LinkedHashMap<>());
        }

        private CrashSimulatingStorage(final Map<String, byte[]> workingFiles,
                final Map<String, byte[]> durableFiles) {
            this.workingFiles = workingFiles;
            this.durableFiles = durableFiles;
        }

        private synchronized CrashSimulatingStorage crashRecover() {
            final Map<String, byte[]> recovered = deepCopy(durableFiles);
            return new CrashSimulatingStorage(recovered, deepCopy(recovered));
        }

        @Override
        public synchronized boolean exists(final String fileName) {
            return workingFiles.containsKey(fileName);
        }

        @Override
        public synchronized void touch(final String fileName) {
            workingFiles.putIfAbsent(fileName, new byte[0]);
        }

        @Override
        public synchronized long size(final String fileName) {
            final byte[] bytes = workingFiles.get(fileName);
            return bytes == null ? 0L : bytes.length;
        }

        @Override
        public synchronized void append(final String fileName, final byte[] bytes,
                final int offset, final int length) {
            final byte[] current = workingFiles.get(fileName);
            if (current == null) {
                throw new IndexException(
                        String.format("Missing WAL file '%s'.", fileName));
            }
            final byte[] appended = Arrays.copyOf(current,
                    current.length + length);
            System.arraycopy(bytes, offset, appended, current.length, length);
            workingFiles.put(fileName, appended);
        }

        @Override
        public synchronized void overwrite(final String fileName,
                final byte[] bytes, final int offset, final int length) {
            final byte[] overwritten = new byte[length];
            System.arraycopy(bytes, offset, overwritten, 0, length);
            workingFiles.put(fileName, overwritten);
        }

        @Override
        public synchronized byte[] readAll(final String fileName) {
            final byte[] bytes = workingFiles.get(fileName);
            return bytes == null ? new byte[0] : Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public synchronized int read(final String fileName, final long position,
                final byte[] destination, final int offset, final int length) {
            final byte[] data = workingFiles.get(fileName);
            if (data == null || position >= data.length) {
                return -1;
            }
            final int available = (int) Math.min(length, data.length - position);
            if (available <= 0) {
                return -1;
            }
            System.arraycopy(data, (int) position, destination, offset, available);
            return available;
        }

        @Override
        public synchronized void truncate(final String fileName,
                final long sizeBytes) {
            final byte[] current = workingFiles.get(fileName);
            if (current == null) {
                return;
            }
            final int targetSize = (int) Math.max(0L,
                    Math.min(sizeBytes, current.length));
            workingFiles.put(fileName, Arrays.copyOf(current, targetSize));
        }

        @Override
        public synchronized boolean delete(final String fileName) {
            return workingFiles.remove(fileName) != null;
        }

        @Override
        public synchronized void rename(final String currentFileName,
                final String newFileName) {
            final byte[] bytes = workingFiles.remove(currentFileName);
            if (bytes == null) {
                throw new IndexException(String.format(
                        "Unable to rename missing WAL file '%s'.",
                        currentFileName));
            }
            workingFiles.put(newFileName, bytes);
        }

        @Override
        public synchronized java.util.stream.Stream<String> listFileNames() {
            return workingFiles.keySet().stream().sorted();
        }

        @Override
        public synchronized void sync(final String fileName) {
            final byte[] bytes = workingFiles.get(fileName);
            if (bytes == null) {
                return;
            }
            durableFiles.put(fileName, Arrays.copyOf(bytes, bytes.length));
        }

        @Override
        public synchronized void syncMetadata() {
            final Map<String, byte[]> synced = new LinkedHashMap<>();
            for (final Map.Entry<String, byte[]> entry : workingFiles.entrySet()) {
                final byte[] durable = durableFiles.get(entry.getKey());
                if (durable != null) {
                    synced.put(entry.getKey(),
                            Arrays.copyOf(durable, durable.length));
                } else {
                    synced.put(entry.getKey(), new byte[0]);
                }
            }
            durableFiles.clear();
            durableFiles.putAll(synced);
        }

        private static Map<String, byte[]> deepCopy(
                final Map<String, byte[]> source) {
            final Map<String, byte[]> copy = new LinkedHashMap<>();
            for (final Map.Entry<String, byte[]> entry : source.entrySet()) {
                copy.put(entry.getKey(),
                        Arrays.copyOf(entry.getValue(), entry.getValue().length));
            }
            return copy;
        }
    }
}
