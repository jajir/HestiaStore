package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.FsNioDirectory;
import org.hestiastore.index.segmentindex.Wal;
import org.junit.jupiter.api.Test;

class WalToolTest {

    private static final TypeDescriptorString STRING_DESCRIPTOR = new TypeDescriptorString();

    @Test
    void verifyPassesForValidWal() throws IOException {
        final Path root = Files.createTempDirectory("hestia-wal-tool-valid-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
            runtime.appendDelete("a");
            runtime.onCheckpoint(2L);
        }
        final WalTool.VerifyResult result = WalTool.verify(root.resolve("wal"));
        assertTrue(result.ok());
        assertTrue(result.recordCount() >= 1L);
    }

    @Test
    void verifyFailsForCorruptedWalTail() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-corrupted-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        final Path segment;
        try (java.util.stream.Stream<Path> listing = Files.list(walDir)) {
            segment = listing
                    .filter(path -> path.getFileName().toString()
                            .endsWith(".wal"))
                    .findFirst().orElseThrow();
        }
        Files.write(segment, new byte[] { 0x01, 0x02, 0x03 },
                StandardOpenOption.APPEND);

        final WalTool.VerifyResult result = WalTool.verify(walDir);
        assertFalse(result.ok());
        assertTrue(result.errorMessage() != null && !result.errorMessage().isBlank());
    }

    @Test
    void verifyFailsWhenFormatMetadataIsMissing() {
        final Path root = createTempWalDirectoryWithoutFormat();
        final Path walDir = root.resolve("wal");
        try {
            Files.write(walDir.resolve("00000000000000000001.wal"),
                    new byte[] { 0x01, 0x02, 0x03 });
            final WalTool.VerifyResult result = WalTool.verify(walDir);
            assertFalse(result.ok());
            assertTrue("format.meta".equals(result.errorFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void verifyFailsForInvalidCheckpointMetadata() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-invalid-checkpoint-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        Files.writeString(walDir.resolve("checkpoint.meta"), "not-a-number",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);

        final WalTool.VerifyResult result = WalTool.verify(walDir);
        assertFalse(result.ok());
        assertTrue("checkpoint.meta".equals(result.errorFile()));
    }

    private static Path createTempWalDirectoryWithoutFormat() {
        try {
            final Path root = Files
                    .createTempDirectory("hestia-wal-tool-missing-format-");
            Files.createDirectories(root.resolve("wal"));
            return root;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
