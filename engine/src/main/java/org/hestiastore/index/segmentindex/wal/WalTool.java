package org.hestiastore.index.segmentindex.wal;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;

/**
 * Command-line helper for WAL inspection and verification.
 */
@SuppressWarnings({ "java:S3776", "java:S6541" })
public final class WalTool {

    private static final String JSON_FILE_FIELD = "\"file\":";
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    private WalTool() {
    }

    @SuppressWarnings("java:S106")
    public static void main(final String[] args) {
        final int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(final String[] args, final PrintStream out,
            final PrintStream err) {
        if (args == null || args.length < 2) {
            printUsage(out);
            return 1;
        }
        if (!hasValidOutputMode(args)) {
            printUsage(out);
            return 1;
        }
        final boolean jsonOutput = args.length == 3;
        final String command = args[0];
        final Path walDirectory = Path.of(args[1]);
        if (!Files.isDirectory(walDirectory)) {
            err.println("WAL tool failed: WAL directory does not exist: "
                    + walDirectory);
            return 1;
        }
        try {
            if ("verify".equals(command)) {
                final VerifyResult result = verify(walDirectory);
                printVerifyResult(result, out, jsonOutput);
                return result.ok() ? 0 : 2;
            }
            if ("dump".equals(command)) {
                dump(walDirectory, out, jsonOutput);
                return 0;
            }
            printUsage(out);
            return 1;
        } catch (RuntimeException e) {
            err.println("WAL tool failed: " + e.getMessage());
            return 1;
        }
    }

    static VerifyResult verify(final Path walDirectory) {
        final VerifyResult formatResult = verifyFormatMetadata(walDirectory);
        if (!formatResult.ok()) {
            return formatResult;
        }
        final CheckpointValidation checkpointValidation = readCheckpointMetadata(
                walDirectory);
        if (checkpointValidation.hasError()) {
            return checkpointValidation.error();
        }
        final SegmentDiscovery segmentDiscovery = discoverSegments(walDirectory);
        if (segmentDiscovery.hasError()) {
            return segmentDiscovery.error();
        }
        final List<Path> files = segmentDiscovery.files();
        long records = 0L;
        long maxLsn = 0L;
        long previousLsn = 0L;
        for (final Path file : files) {
            final WalFileScan scan = scanFile(file, previousLsn, true);
            records += scan.recordCount();
            maxLsn = Math.max(maxLsn, scan.lastLsn());
            if (scan.lastLsn() > 0L) {
                previousLsn = scan.lastLsn();
            }
            if (scan.hasInvalidTail()) {
                return new VerifyResult(false, files.size(), records, maxLsn,
                        scan.fileName(), scan.invalidOffset(),
                        scan.invalidReason());
            }
        }
        if (checkpointValidation.hasCheckpoint()
                && checkpointValidation.checkpointLsn() > maxLsn) {
            return new VerifyResult(false, files.size(), records, maxLsn,
                    WalMetadataCatalog.CHECKPOINT_FILE, -1L,
                    "Checkpoint LSN is ahead of max WAL LSN.");
        }
        return new VerifyResult(true, files.size(), records, maxLsn, null, -1L,
                null);
    }

    static void dump(final Path walDirectory, final PrintStream out) {
        dump(walDirectory, out, false);
    }

    static void dump(final Path walDirectory, final PrintStream out,
            final boolean jsonOutput) {
        final VerifyResult formatResult = verifyFormatMetadata(walDirectory);
        if (!formatResult.ok()) {
            throw metadataError(formatResult);
        }
        final CheckpointValidation checkpointValidation = readCheckpointMetadata(
                walDirectory);
        if (checkpointValidation.hasError()) {
            throw metadataError(checkpointValidation.error());
        }
        final SegmentDiscovery segmentDiscovery = discoverSegments(walDirectory);
        if (segmentDiscovery.hasError()) {
            throw new IndexException(segmentDiscovery.error().errorMessage());
        }
        final List<Path> files = segmentDiscovery.files();
        for (final Path file : files) {
            final WalFileScan scan = scanFile(file, 0L, false);
            for (final WalToolRecord walRecord : scan.records()) {
                if (jsonOutput) {
                    out.println(jsonRecord(scan.fileName(), walRecord.offset(),
                            walRecord.lsn(), walRecord.operation(),
                            walRecord.keyLen(), walRecord.valueLen(),
                            walRecord.bodyLen()));
                } else {
                    out.println("record file=" + scan.fileName() + " offset="
                            + walRecord.offset() + " lsn=" + walRecord.lsn()
                            + " op=" + walRecord.operation() + " keyLen="
                            + walRecord.keyLen() + " valueLen="
                            + walRecord.valueLen() + " bodyLen="
                            + walRecord.bodyLen());
                }
            }
            if (scan.hasInvalidTail()) {
                if (jsonOutput) {
                    out.println(jsonInvalidTail(scan.fileName(),
                            scan.invalidOffset(), scan.invalidReason()));
                } else {
                    out.println("invalid file=" + scan.fileName() + " offset="
                            + scan.invalidOffset() + " reason="
                            + scan.invalidReason());
                }
            }
            if (jsonOutput) {
                out.println(jsonSummary(scan.fileName(), scan.size(),
                        scan.recordCount(), scan.firstLsn(), scan.lastLsn()));
            } else {
                out.println("summary file=" + scan.fileName() + " size="
                        + scan.size() + " records=" + scan.recordCount()
                        + " firstLsn=" + scan.firstLsn() + " lastLsn="
                        + scan.lastLsn());
            }
        }
    }

    private static WalFileScan scanFile(final Path file,
            final long previousLsn, final boolean validateLsnOrdering) {
        final byte[] bytes = readAll(file);
        final String fileName = file.getFileName().toString();
        final List<WalToolRecord> records = new ArrayList<>();
        long offset = 0L;
        long lastLsn = previousLsn;
        while (offset < bytes.length) {
            if (bytes.length - offset < 4L) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "Partial record length prefix.");
            }
            final int bodyLen = WalRecordCodec.readInt(bytes, (int) offset);
            if (bodyLen < WalRecordCodec.MIN_RECORD_BODY_SIZE
                    || bodyLen > WalRecordCodec.MAX_RECORD_BODY_SIZE) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "Invalid WAL record body length.");
            }
            if (offset + 4L + bodyLen > bytes.length) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "Truncated WAL record body.");
            }
            final int bodyOffset = (int) (offset + 4L);
            final int storedCrc = WalRecordCodec.readInt(bytes, bodyOffset);
            final int computedCrc = WalRecordCodec.computeCrc32(bytes,
                    bodyOffset + 4, bodyLen - 4);
            if (storedCrc != computedCrc) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "CRC mismatch.");
            }
            int position = bodyOffset + 4;
            final long lsn = WalRecordCodec.readLong(bytes, position);
            position += 8;
            final byte opCode = bytes[position++];
            if (!isSupportedOperationCode(opCode)) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "Invalid WAL operation code.");
            }
            final int keyLen = WalRecordCodec.readInt(bytes, position);
            position += 4;
            final int valueLen = WalRecordCodec.readInt(bytes, position);
            position += 4;
            if (keyLen <= 0 || valueLen < 0
                    || position + keyLen + valueLen != bodyOffset + bodyLen) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "Invalid key/value frame lengths.");
            }
            if (isDeleteOperationCode(opCode) && valueLen != 0) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "DELETE record must have zero value length.");
            }
            if (validateLsnOrdering
                    && (lsn <= 0L || lastLsn > 0L && lsn <= lastLsn)) {
                return WalFileScan.invalid(fileName, bytes.length, records,
                        offset, "Non-monotonic or invalid LSN.");
            }
            records.add(new WalToolRecord(offset, lsn, operationName(opCode),
                    keyLen, valueLen, bodyLen));
            lastLsn = lsn;
            offset += 4L + bodyLen;
        }
        return WalFileScan.valid(fileName, bytes.length, records);
    }

    private static SegmentDiscovery discoverSegments(final Path walDirectory) {
        final List<Path> candidates;
        try (Stream<Path> listing = Files.list(walDirectory)) {
            candidates = listing
                    .filter(path -> path.getFileName().toString()
                            .endsWith(WalMetadataCatalog.SEGMENT_SUFFIX))
                    .sorted(Comparator.comparing(Path::getFileName)).toList();
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to list WAL directory '%s'.",
                            walDirectory),
                    e);
        }

        final Set<Long> uniqueBaseLsns = new HashSet<>();
        for (final Path file : candidates) {
            final String fileName = file.getFileName().toString();
            if (!Files.isRegularFile(file)) {
                return new SegmentDiscovery(List.of(),
                        new VerifyResult(false, 0, 0L, 0L, fileName, -1L,
                                "WAL segment entry is not a regular file."));
            }
            final long baseLsn = WalMetadataCatalog.parseSegmentBaseLsn(
                    fileName);
            if (baseLsn < 0L) {
                return new SegmentDiscovery(List.of(),
                        new VerifyResult(false, 0, 0L, 0L, fileName, -1L,
                                "Invalid WAL segment file name. Expected 20 numeric digits plus '.wal'."));
            }
            if (!uniqueBaseLsns.add(baseLsn)) {
                return new SegmentDiscovery(List.of(),
                        new VerifyResult(false, 0, 0L, 0L, fileName, -1L,
                                "Duplicate WAL segment base LSN."));
            }
        }
        return new SegmentDiscovery(candidates, null);
    }

    private static boolean isSupportedOperationCode(final byte opCode) {
        return opCode == 1 || opCode == 2;
    }

    private static boolean isDeleteOperationCode(final byte opCode) {
        return opCode == 2;
    }

    private static String operationName(final byte opCode) {
        return isDeleteOperationCode(opCode) ? "DELETE" : "PUT";
    }

    private static byte[] readAll(final Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to read WAL file '%s'.", file), e);
        }
    }

    private static void printUsage(final PrintStream out) {
        out.println("Usage: WalTool <verify|dump> <walDirectoryPath> [--json]");
    }

    private static boolean hasValidOutputMode(final String[] args) {
        return args.length == 2 || args.length == 3 && "--json".equals(args[2]);
    }

    private static void printVerifyResult(final VerifyResult result,
            final PrintStream out, final boolean jsonOutput) {
        if (jsonOutput) {
            out.println(jsonVerifyResult(result));
            return;
        }
        out.println("verify.ok=" + result.ok());
        out.println("verify.files=" + result.fileCount());
        out.println("verify.records=" + result.recordCount());
        out.println("verify.maxLsn=" + result.maxLsn());
        if (!result.ok()) {
            out.println("verify.errorFile=" + result.errorFile());
            out.println("verify.errorOffset=" + result.errorOffset());
            out.println("verify.errorMessage=" + result.errorMessage());
        }
    }

    private static String jsonVerifyResult(final VerifyResult result) {
        return "{\"type\":\"verify\","
                + "\"ok\":" + result.ok() + ","
                + "\"files\":" + result.fileCount() + ","
                + "\"records\":" + result.recordCount() + ","
                + "\"maxLsn\":" + result.maxLsn() + ","
                + "\"errorFile\":" + toJsonString(result.errorFile()) + ","
                + "\"errorOffset\":" + result.errorOffset() + ","
                + "\"errorMessage\":" + toJsonString(result.errorMessage()) + "}";
    }

    private static String jsonRecord(final String fileName, final long offset,
            final long lsn, final String operation, final int keyLen,
            final int valueLen, final int bodyLen) {
        return "{\"type\":\"record\","
                + JSON_FILE_FIELD + toJsonString(fileName) + ","
                + "\"offset\":" + offset + ","
                + "\"lsn\":" + lsn + ","
                + "\"op\":" + toJsonString(operation) + ","
                + "\"keyLen\":" + keyLen + ","
                + "\"valueLen\":" + valueLen + ","
                + "\"bodyLen\":" + bodyLen + "}";
    }

    private static String jsonInvalidTail(final String fileName,
            final long invalidOffset, final String reason) {
        return "{\"type\":\"invalid_tail\","
                + JSON_FILE_FIELD + toJsonString(fileName) + ","
                + "\"offset\":" + invalidOffset + ","
                + "\"reason\":" + toJsonString(reason) + "}";
    }

    private static String jsonSummary(final String fileName, final long size,
            final long records, final long firstLsn, final long lastLsn) {
        return "{\"type\":\"summary\","
                + JSON_FILE_FIELD + toJsonString(fileName) + ","
                + "\"size\":" + size + ","
                + "\"records\":" + records + ","
                + "\"firstLsn\":" + firstLsn + ","
                + "\"lastLsn\":" + lastLsn + "}";
    }

    private static String toJsonString(final String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + escapeJson(value) + "\"";
    }

    private static String escapeJson(final String value) {
        final StringBuilder escaped = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            final char ch = value.charAt(i);
            switch (ch) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        appendUnicodeEscape(escaped, ch);
                    } else {
                        escaped.append(ch);
                    }
                }
            }
        }
        return escaped.toString();
    }

    private static void appendUnicodeEscape(final StringBuilder target,
            final char ch) {
        target.append("\\u").append(HEX_DIGITS[ch >>> 12 & 0xF])
                .append(HEX_DIGITS[ch >>> 8 & 0xF])
                .append(HEX_DIGITS[ch >>> 4 & 0xF])
                .append(HEX_DIGITS[ch & 0xF]);
    }

    private static VerifyResult verifyFormatMetadata(final Path walDirectory) {
        final Path formatFile = walDirectory.resolve(WalMetadataCatalog.FORMAT_FILE);
        if (Files.exists(formatFile)) {
            return verifyFormatMetadataFile(formatFile,
                    WalMetadataCatalog.FORMAT_FILE);
        }
        final Path formatTmpFile = walDirectory.resolve(
                WalMetadataCatalog.FORMAT_FILE_TMP);
        if (Files.exists(formatTmpFile)) {
            return verifyFormatMetadataFile(formatTmpFile,
                    WalMetadataCatalog.FORMAT_FILE_TMP);
        }
        return VerifyResult.failure(0, 0L, 0L, WalMetadataCatalog.FORMAT_FILE,
                -1L, "Missing WAL format metadata.");
    }

    private static VerifyResult verifyFormatMetadataFile(final Path path,
            final String errorFile) {
        try {
            WalMetadataCatalog.validateFormatMetadata(readAll(path));
            return VerifyResult.ok(0, 0L, 0L);
        } catch (final RuntimeException e) {
            return VerifyResult.failure(0, 0L, 0L, errorFile, -1L,
                    e.getMessage());
        }
    }

    private static CheckpointValidation readCheckpointMetadata(
            final Path walDirectory) {
        final Path checkpointFile = walDirectory.resolve(
                WalMetadataCatalog.CHECKPOINT_FILE);
        if (Files.exists(checkpointFile)) {
            return parseCheckpointMetadataFile(checkpointFile,
                    WalMetadataCatalog.CHECKPOINT_FILE, true);
        }
        final Path checkpointTmpFile = walDirectory.resolve(
                WalMetadataCatalog.CHECKPOINT_FILE_TMP);
        if (Files.exists(checkpointTmpFile)) {
            return parseCheckpointMetadataFile(checkpointTmpFile,
                    WalMetadataCatalog.CHECKPOINT_FILE_TMP, false);
        }
        return CheckpointValidation.none();
    }

    private static CheckpointValidation parseCheckpointMetadataFile(
            final Path path, final String errorFile,
            final boolean failOnInvalidMetadata) {
        final byte[] bytes = readAll(path);
        final String text = new String(bytes, StandardCharsets.US_ASCII).trim();
        if (text.isEmpty()) {
            return CheckpointValidation.none();
        }
        try {
            return CheckpointValidation.checkpoint(
                    WalMetadataCatalog.parseCheckpointMetadata(bytes));
        } catch (final RuntimeException e) {
            return checkpointValidationError(errorFile,
                    e.getMessage(), failOnInvalidMetadata);
        }
    }

    private static CheckpointValidation checkpointValidationError(
            final String errorFile, final String message,
            final boolean failOnInvalidMetadata) {
        if (!failOnInvalidMetadata) {
            return CheckpointValidation.none();
        }
        return CheckpointValidation.error(VerifyResult.failure(0, 0L, 0L,
                errorFile, -1L, message));
    }

    private static IndexException metadataError(final VerifyResult error) {
        return new IndexException(String.format("%s (%s)", error.errorMessage(),
                error.errorFile()));
    }

    static final class VerifyResult {
        private final boolean ok;
        private final int fileCount;
        private final long recordCount;
        private final long maxLsn;
        private final String errorFile;
        private final long errorOffset;
        private final String errorMessage;

        VerifyResult(final boolean ok, final int fileCount,
                final long recordCount, final long maxLsn,
                final String errorFile, final long errorOffset,
                final String errorMessage) {
            this.ok = ok;
            this.fileCount = fileCount;
            this.recordCount = recordCount;
            this.maxLsn = maxLsn;
            this.errorFile = errorFile;
            this.errorOffset = errorOffset;
            this.errorMessage = errorMessage;
        }

        static VerifyResult ok(final int fileCount, final long recordCount,
                final long maxLsn) {
            return new VerifyResult(true, fileCount, recordCount, maxLsn, null,
                    -1L, null);
        }

        static VerifyResult failure(final int fileCount,
                final long recordCount, final long maxLsn,
                final String errorFile, final long errorOffset,
                final String errorMessage) {
            return new VerifyResult(false, fileCount, recordCount, maxLsn,
                    errorFile, errorOffset, errorMessage);
        }

        boolean ok() {
            return ok;
        }

        int fileCount() {
            return fileCount;
        }

        long recordCount() {
            return recordCount;
        }

        long maxLsn() {
            return maxLsn;
        }

        String errorFile() {
            return errorFile;
        }

        long errorOffset() {
            return errorOffset;
        }

        String errorMessage() {
            return errorMessage;
        }
    }

    private static final class SegmentDiscovery {
        private final List<Path> files;
        private final VerifyResult error;

        SegmentDiscovery(final List<Path> files, final VerifyResult error) {
            this.files = List.copyOf(files);
            this.error = error;
        }

        List<Path> files() {
            return files;
        }

        VerifyResult error() {
            return error;
        }

        boolean hasError() {
            return error != null;
        }
    }

    private static final class CheckpointValidation {
        private final boolean hasCheckpoint;
        private final long checkpointLsn;
        private final VerifyResult error;

        private CheckpointValidation(final boolean hasCheckpoint,
                final long checkpointLsn, final VerifyResult error) {
            this.hasCheckpoint = hasCheckpoint;
            this.checkpointLsn = checkpointLsn;
            this.error = error;
        }

        static CheckpointValidation none() {
            return new CheckpointValidation(false, 0L, null);
        }

        static CheckpointValidation checkpoint(final long checkpointLsn) {
            return new CheckpointValidation(true, checkpointLsn, null);
        }

        static CheckpointValidation error(final VerifyResult error) {
            return new CheckpointValidation(false, 0L, error);
        }

        boolean hasCheckpoint() {
            return hasCheckpoint;
        }

        long checkpointLsn() {
            return checkpointLsn;
        }

        VerifyResult error() {
            return error;
        }

        boolean hasError() {
            return error != null;
        }
    }

}
