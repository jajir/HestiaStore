package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
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
    void syncFailureFailsCurrentAndFollowingWrites() {
        final Wal wal = Wal.builder().withEnabled(true)
                .withDurabilityMode(WalDurabilityMode.SYNC).build();
        final WalStorage failingStorage = new FailingSyncStorage(
                new WalStorageMem(new MemDirectory()));
        try (WalRuntime<String, String> runtime = WalRuntime.openForTests(wal,
                failingStorage, STRING_DESCRIPTOR, STRING_DESCRIPTOR)) {
            assertThrows(IndexException.class,
                    () -> runtime.appendPut("k1", "v1"));
            assertThrows(IndexException.class,
                    () -> runtime.appendPut("k2", "v2"));
        }
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
        try (org.hestiastore.index.directory.FileWriter writer = walDirectory
                .getFileWriter(segmentName, Directory.Access.APPEND)) {
            writer.write(new byte[] { 0x01, 0x02, 0x03, 0x04 });
        }
    }

    private static final class FailingSyncStorage implements WalStorage {

        private final WalStorage delegate;

        private FailingSyncStorage(final WalStorage delegate) {
            this.delegate = delegate;
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
                throw new IndexException("simulated sync failure");
            }
            delegate.sync(fileName);
        }
    }
}
