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
    }
}
