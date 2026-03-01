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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.hestiastore.index.segmentindex.WalReplicationMode;
import org.junit.jupiter.api.Test;

class WalRuntimeTest {

    private static final TypeDescriptorString STRING_DESCRIPTOR = new TypeDescriptorString();

    @Test
    void recoverTruncatesInvalidTailAndReplaysValidPrefix() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
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
                .withEnabled(true)//
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
                .withEnabled(true)//
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
                .withEnabled(true)//
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
                    .recover(record -> {
                    });
            assertTrue(result.truncatedTail());
            assertEquals(0L, result.maxLsn());
        }
    }

    @Test
    void recoverFailFastOnInvalidOperationCodeWhenConfigured() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
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
                    () -> runtime.recover(record -> {
                    }));
        }
    }

    @Test
    void checkpointDeletesSealedSegmentsAfterRotation() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
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
    void retentionPressureClearsAfterCheckpointCleanup() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
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
                .withEnabled(true)//
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
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> ignored = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final Directory walDirectory = root.openSubDirectory("wal");
            assertTrue(walDirectory.isFileExists("format.meta"));
        }
    }

    @Test
    void openCreatesEpochMarkerWhenEpochSupportEnabled() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).withEpochSupport(true)
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            final Directory walDirectory = root.openSubDirectory("wal");
            assertTrue(walDirectory.isFileExists("epoch.meta"));
            assertEquals(0L, runtime.currentEpoch());
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
        final Wal wal = Wal.builder().withEnabled(true).build();

        assertThrows(IndexException.class, () -> WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void bumpEpochPersistsAcrossRestartAndFencesStaleAppends() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).withEpochSupport(true)
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.bumpEpoch(5L);
            runtime.appendPutWithEpoch(5L, "k1", "v1");
            assertThrows(IndexException.class,
                    () -> runtime.appendPutWithEpoch(4L, "k2", "v2"));
        }

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.recover(replayed::add);
            assertEquals(5L, runtime.currentEpoch());
            runtime.appendPutWithEpoch(5L, "k3", "v3");
        }
        assertEquals(1, replayed.size());
        assertEquals("k1", replayed.get(0).getKey());
        assertEquals("v1", replayed.get(0).getValue());
    }

    @Test
    void bumpEpochRejectsNonMonotonicValue() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).withEpochSupport(true)
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.bumpEpoch(2L);
            assertThrows(IllegalArgumentException.class,
                    () -> runtime.bumpEpoch(2L));
            assertThrows(IllegalArgumentException.class,
                    () -> runtime.bumpEpoch(1L));
        }
    }

    @Test
    void fetchRecordsReturnsWindowAfterLsnWithByteLimit() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            for (int i = 1; i <= 5; i++) {
                runtime.appendPut("k" + i, "v" + i);
            }
            final List<WalRuntime.ReplicatedRecord> all = runtime.fetchRecords(0L,
                    1024 * 1024);
            assertEquals(5, all.size());
            assertEquals(1L, all.get(0).getLsn());
            assertEquals(5L, all.get(4).getLsn());

            final int firstRecordBytes = all.get(0).getEncodedRecord().length;
            final List<WalRuntime.ReplicatedRecord> limited = runtime
                    .fetchRecords(0L, firstRecordBytes);
            assertEquals(1, limited.size());
            assertEquals(1L, limited.get(0).getLsn());

            final List<WalRuntime.ReplicatedRecord> afterTwo = runtime
                    .fetchRecords(2L, 1024 * 1024);
            assertEquals(3, afterTwo.size());
            assertEquals(3L, afterTwo.get(0).getLsn());
            assertEquals(5L, afterTwo.get(2).getLsn());
        }
    }

    @Test
    void appendReplicatedOnFollowerAcceptsContiguousRecords() {
        final MemDirectory leaderRoot = new MemDirectory();
        final Wal leaderWal = Wal.builder().withEnabled(true)
                .withEpochSupport(true)
                .withReplicationMode(WalReplicationMode.LEADER)
                .withSourceNodeId("node-leader")
                .build();
        final List<WalRuntime.ReplicatedRecord> shipped;
        try (WalRuntime<String, String> leader = WalRuntime.open(leaderRoot,
                leaderWal, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            leader.bumpEpoch(7L);
            leader.appendPutWithEpoch(7L, "k1", "v1");
            leader.appendDeleteWithEpoch(7L, "k2");
            leader.appendPutWithEpoch(7L, "k3", "v3");
            shipped = leader.fetchRecords(0L, 1024 * 1024);
        }
        assertEquals(3, shipped.size());

        final MemDirectory followerRoot = new MemDirectory();
        final Wal followerWal = Wal.builder().withEnabled(true)
                .withEpochSupport(true)
                .withReplicationMode(WalReplicationMode.FOLLOWER)
                .withSourceNodeId("node-leader")
                .build();
        final long lastReplicatedLsn;
        try (WalRuntime<String, String> follower = WalRuntime.open(followerRoot,
                followerWal, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            follower.bumpEpoch(7L);
            lastReplicatedLsn = follower.appendReplicated(
                    toEncodedRecords(shipped), 7L, "node-leader");
        }
        assertEquals(shipped.get(shipped.size() - 1).getLsn(),
                lastReplicatedLsn);

        final List<WalRuntime.ReplayRecord<String, String>> replayed = new ArrayList<>();
        try (WalRuntime<String, String> follower = WalRuntime.open(followerRoot,
                followerWal, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            follower.recover(replayed::add);
        }
        assertEquals(3, replayed.size());
        assertEquals(WalRuntime.Operation.PUT, replayed.get(0).getOperation());
        assertEquals("k1", replayed.get(0).getKey());
        assertEquals("v1", replayed.get(0).getValue());
        assertEquals(WalRuntime.Operation.DELETE,
                replayed.get(1).getOperation());
        assertEquals("k2", replayed.get(1).getKey());
        assertEquals(WalRuntime.Operation.PUT, replayed.get(2).getOperation());
        assertEquals("k3", replayed.get(2).getKey());
        assertEquals("v3", replayed.get(2).getValue());
    }

    @Test
    void appendReplicatedRejectsNonContiguousStream() {
        final MemDirectory leaderRoot = new MemDirectory();
        final Wal leaderWal = Wal.builder().withEnabled(true)
                .withEpochSupport(true)
                .withReplicationMode(WalReplicationMode.LEADER)
                .withSourceNodeId("node-leader")
                .build();
        final List<WalRuntime.ReplicatedRecord> shipped;
        try (WalRuntime<String, String> leader = WalRuntime.open(leaderRoot,
                leaderWal, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            leader.bumpEpoch(11L);
            leader.appendPutWithEpoch(11L, "k1", "v1");
            leader.appendPutWithEpoch(11L, "k2", "v2");
            shipped = leader.fetchRecords(0L, 1024 * 1024);
        }
        final List<byte[]> malformed = new ArrayList<>(
                toEncodedRecords(shipped));
        final long firstLsn = shipped.get(0).getLsn();
        malformed.set(1, withEncodedRecordLsn(malformed.get(1), firstLsn + 2));

        final MemDirectory followerRoot = new MemDirectory();
        final Wal followerWal = Wal.builder().withEnabled(true)
                .withEpochSupport(true)
                .withReplicationMode(WalReplicationMode.FOLLOWER)
                .withSourceNodeId("node-leader")
                .build();
        try (WalRuntime<String, String> follower = WalRuntime.open(followerRoot,
                followerWal, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            follower.bumpEpoch(11L);
            assertThrows(IndexException.class, () -> follower.appendReplicated(
                    malformed, 11L, "node-leader"));
        }
    }

    @Test
    void appendReplicatedRejectsWrongSourceOrEpoch() {
        final MemDirectory leaderRoot = new MemDirectory();
        final Wal leaderWal = Wal.builder().withEnabled(true)
                .withEpochSupport(true)
                .withReplicationMode(WalReplicationMode.LEADER)
                .withSourceNodeId("node-leader")
                .build();
        final List<byte[]> shipped;
        try (WalRuntime<String, String> leader = WalRuntime.open(leaderRoot,
                leaderWal, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            leader.bumpEpoch(15L);
            leader.appendPutWithEpoch(15L, "k1", "v1");
            shipped = toEncodedRecords(leader.fetchRecords(0L, 1024 * 1024));
        }

        final MemDirectory followerRoot = new MemDirectory();
        final Wal followerWal = Wal.builder().withEnabled(true)
                .withEpochSupport(true)
                .withReplicationMode(WalReplicationMode.FOLLOWER)
                .withSourceNodeId("node-leader")
                .build();
        try (WalRuntime<String, String> follower = WalRuntime.open(followerRoot,
                followerWal, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            follower.bumpEpoch(15L);
            assertThrows(IndexException.class, () -> follower.appendReplicated(
                    shipped, 15L, "node-other"));
            assertThrows(IndexException.class, () -> follower.appendReplicated(
                    shipped, 14L, "node-leader"));
        }
    }

    @Test
    void appendReplicatedRejectsWhenModeIsNotFollower() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true)
                .withEpochSupport(true)
                .withReplicationMode(WalReplicationMode.LEADER)
                .withSourceNodeId("node-leader")
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.bumpEpoch(21L);
            runtime.appendPutWithEpoch(21L, "k1", "v1");
            final byte[] encoded = runtime.fetchRecords(0L, 1024 * 1024).get(0)
                    .getEncodedRecord();
            assertThrows(IndexException.class,
                    () -> runtime.appendReplicated(List.of(encoded), 21L,
                            "node-leader"));
        }
    }

    @Test
    void recoverFailsWhenCheckpointMetadataIsNonNumeric() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        writeCheckpointMetadata(root, "not-a-number");

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(record -> {
                    }));
        }
    }

    @Test
    void recoverFailsWhenEpochMetadataIsNonNumeric() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).withEpochSupport(true)
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        writeEpochMetadata(root, "not-a-number");

        assertThrows(IndexException.class, () -> WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void recoverFailsWhenCheckpointMetadataIsNegative() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        writeCheckpointMetadata(root, "-1");

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(record -> {
                    }));
        }
    }

    @Test
    void recoverFailsWhenEpochMetadataIsNegative() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder().withEnabled(true).withEpochSupport(true)
                .build();
        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            runtime.appendPut("k1", "v1");
        }
        writeEpochMetadata(root, "-1");

        assertThrows(IndexException.class, () -> WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR));
    }

    @Test
    void recoverFailsWhenStorageReadFails() {
        final Wal wal = Wal.builder().withEnabled(true).build();
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
                    () -> runtime.recover(record -> {
                    }));
        }
    }

    @Test
    void checkpointFailsWhenStorageRenameFails() {
        final Wal wal = Wal.builder().withEnabled(true).build();
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
    void checkpointCleanupFailsWhenStorageDeleteFails() {
        final Wal wal = Wal.builder().withEnabled(true)
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
            runtime.recover(record -> {
            });
            assertThrows(IndexException.class,
                    () -> runtime.onCheckpoint(checkpointLsn));
        }
    }

    @Test
    void recoverRejectsDuplicateSegmentBaseLsn() {
        final MemDirectory root = new MemDirectory();
        final Directory walDirectory = root.openSubDirectory("wal");
        walDirectory.touch("00000000000000000001.wal");
        walDirectory.touch("1.wal");
        final Wal wal = Wal.builder().withEnabled(true).build();

        try (WalRuntime<String, String> runtime = WalRuntime.open(root, wal,
                STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.recover(record -> {
                    }));
        }
    }

    @Test
    void recoverDeletesStaleSegmentsAfterCorruptionAndPersistsRepair() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
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
                    .recover(record -> {
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
    void recoverTruncatesWhenSegmentLsnRegressesAcrossBoundary() {
        final MemDirectory root = new MemDirectory();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
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
                .withEnabled(true)//
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
    void syncFailureFailsCurrentAndFollowingWrites() {
        final Wal wal = Wal.builder().withEnabled(true)
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
    void asyncModeMayLoseUnsyncedWritesAfterCrash() {
        final Wal wal = Wal.builder().withEnabled(true)
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
        final Wal wal = Wal.builder().withEnabled(true)
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
        final Wal wal = Wal.builder().withEnabled(true)
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
    void groupSyncSyncsAllPendingRotatedSegments() throws Exception {
        final Wal wal = Wal.builder().withEnabled(true)
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

    private static void writeEpochMetadata(final MemDirectory root,
            final String value) {
        final Directory walDirectory = root.openSubDirectory("wal");
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter("epoch.meta", Directory.Access.OVERWRITE)) {
            writer.write(value.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }
    }

    private static List<byte[]> toEncodedRecords(
            final List<WalRuntime.ReplicatedRecord> records) {
        return records.stream().map(WalRuntime.ReplicatedRecord::getEncodedRecord)
                .toList();
    }

    private static byte[] withEncodedRecordLsn(final byte[] encodedRecord,
            final long lsn) {
        final byte[] updated = Arrays.copyOf(encodedRecord, encodedRecord.length);
        final int bodyLen = WalRuntime.readInt(updated, 0);
        WalRuntime.putLong(updated, 8, lsn);
        final int crc = WalRuntime.computeCrc32(updated, 8, bodyLen - 4);
        WalRuntime.putInt(updated, 4, crc);
        return updated;
    }

    private static final class FailingStorage implements WalStorage {

        private final WalStorage delegate;
        private final Predicate<String> failRead;
        private final Predicate<String> failDelete;
        private final BiPredicate<String, String> failRename;

        private FailingStorage(final WalStorage delegate,
                final Predicate<String> failRead,
                final Predicate<String> failDelete,
                final BiPredicate<String, String> failRename) {
            this.delegate = delegate;
            this.failRead = failRead;
            this.failDelete = failDelete;
            this.failRename = failRename;
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
            delegate.sync(fileName);
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
