package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
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

    @Test
    void verifyFailsWhenCheckpointLsnIsAheadOfWalMaxLsn() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-checkpoint-ahead-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        Files.writeString(walDir.resolve("checkpoint.meta"), "999",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);

        final WalTool.VerifyResult result = WalTool.verify(walDir);
        assertFalse(result.ok());
        assertTrue("checkpoint.meta".equals(result.errorFile()));
        assertTrue(result.errorMessage() != null
                && result.errorMessage().contains("ahead"));
    }

    @Test
    void verifyFailsForInvalidSegmentFileName() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-invalid-segment-name-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        final Path sourceSegment = findFirstSegment(walDir);
        Files.copy(sourceSegment, walDir.resolve("invalid-segment.wal"));

        final WalTool.VerifyResult result = WalTool.verify(walDir);
        assertFalse(result.ok());
        assertTrue("invalid-segment.wal".equals(result.errorFile()));
    }

    @Test
    void verifyFailsForDuplicateSegmentBaseLsn() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-duplicate-segment-base-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        final Path sourceSegment = findFirstSegment(walDir);
        Files.copy(sourceSegment, walDir.resolve("1.wal"));

        final WalTool.VerifyResult result = WalTool.verify(walDir);
        assertFalse(result.ok());
        assertTrue("1.wal".equals(result.errorFile()));
    }

    @Test
    void verifyFailsForNonMonotonicLsnAcrossSegments() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-non-monotonic-lsn-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        final Path sourceSegment = findFirstSegment(walDir);
        Files.copy(sourceSegment, walDir.resolve("00000000000000000002.wal"));

        final WalTool.VerifyResult result = WalTool.verify(walDir);
        assertFalse(result.ok());
        assertTrue("00000000000000000002.wal".equals(result.errorFile()));
        assertTrue(result.errorMessage() != null
                && result.errorMessage().contains("Non-monotonic"));
    }

    @Test
    void dumpPrintsRecordMetadataAndSummary() throws IOException {
        final Path root = Files.createTempDirectory("hestia-wal-tool-dump-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
            runtime.appendDelete("a");
        }
        final String output = captureStdout(
                out -> WalTool.dump(root.resolve("wal"), out));
        assertTrue(output.contains("record file="));
        assertTrue(output.contains("op=PUT"));
        assertTrue(output.contains("op=DELETE"));
        assertTrue(output.contains("summary file="));
    }

    @Test
    void dumpPrintsInvalidTailDetails() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-dump-invalid-tail-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        final Path segment = findFirstSegment(walDir);
        Files.write(segment, new byte[] { 0x01, 0x02, 0x03 },
                StandardOpenOption.APPEND);

        final String output = captureStdout(out -> WalTool.dump(walDir, out));
        assertTrue(output.contains("invalid file="));
        assertTrue(output.contains("reason="));
    }

    @Test
    void runReturnsExitCodeTwoWhenVerifyFails() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-run-verify-fails-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final Path walDir = root.resolve("wal");
        Files.writeString(walDir.resolve("checkpoint.meta"), "999",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);

        final CommandRunResult result = runWalTool("verify",
                walDir.toString());
        assertEquals(2, result.exitCode());
        assertTrue(result.stdout().contains("verify.ok=false"));
        assertTrue(result.stdout().contains("verify.errorMessage="));
    }

    @Test
    void runReturnsExitCodeZeroWhenVerifyPasses() throws IOException {
        final Path root = Files
                .createTempDirectory("hestia-wal-tool-run-verify-passes-");
        final Wal wal = Wal.builder().withEnabled(true).build();
        try (WalRuntime<String, String> runtime = WalRuntime
                .open(new FsNioDirectory(root.toFile()), wal, STRING_DESCRIPTOR,
                        STRING_DESCRIPTOR)) {
            runtime.appendPut("a", "1");
        }
        final CommandRunResult result = runWalTool("verify",
                root.resolve("wal").toString());
        assertEquals(0, result.exitCode());
        assertTrue(result.stdout().contains("verify.ok=true"));
    }

    @Test
    void runReturnsExitCodeOneForInvalidArgs() {
        final CommandRunResult result = runWalTool();
        assertEquals(1, result.exitCode());
        assertTrue(result.stdout().contains("Usage: WalTool"));
    }

    private static Path findFirstSegment(final Path walDir) throws IOException {
        try (java.util.stream.Stream<Path> listing = Files.list(walDir)) {
            return listing
                    .filter(path -> path.getFileName().toString()
                            .endsWith(".wal"))
                    .findFirst().orElseThrow();
        }
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

    private static String captureStdout(
            final java.util.function.Consumer<PrintStream> action) {
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        final PrintStream captureStream = new PrintStream(captured, true,
                StandardCharsets.UTF_8);
        try {
            action.accept(captureStream);
        } finally {
            captureStream.close();
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    private static CommandRunResult runWalTool(final String... args) {
        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(outBytes, true,
                StandardCharsets.UTF_8);
        final PrintStream err = new PrintStream(errBytes, true,
                StandardCharsets.UTF_8);
        try {
            final int exitCode = WalTool.run(args, out, err);
            return new CommandRunResult(exitCode,
                    outBytes.toString(StandardCharsets.UTF_8),
                    errBytes.toString(StandardCharsets.UTF_8));
        } finally {
            out.close();
            err.close();
        }
    }

    private record CommandRunResult(int exitCode, String stdout, String stderr) {
    }
}
