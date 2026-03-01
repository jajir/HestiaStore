package org.hestiastore.index.segmentindex.wal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.io.PrintStream;

import org.hestiastore.index.IndexException;

/**
 * Command-line helper for WAL inspection and verification.
 */
public final class WalTool {

    private static final String SEGMENT_SUFFIX = ".wal";
    private static final String FORMAT_FILE = "format.meta";
    private static final String CHECKPOINT_FILE = "checkpoint.meta";
    private static final int FORMAT_VERSION = 1;
    private static final int MIN_RECORD_BODY_SIZE = 4 + 8 + 1 + 4 + 4;
    private static final int MAX_RECORD_BODY_SIZE = 32 * 1024 * 1024;

    private enum OutputMode {
        TEXT,
        JSON
    }

    private WalTool() {
    }

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
        final OutputMode outputMode = parseOutputMode(args, out);
        if (outputMode == null) {
            return 1;
        }
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
                printVerifyResult(result, out, outputMode);
                return result.ok() ? 0 : 2;
            }
            if ("dump".equals(command)) {
                dump(walDirectory, out, outputMode);
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
        if (formatResult != null) {
            return formatResult;
        }
        final CheckpointValidation checkpointValidation = readCheckpointMetadata(
                walDirectory);
        if (checkpointValidation.error() != null) {
            return checkpointValidation.error();
        }
        final SegmentDiscovery segmentDiscovery = discoverSegments(walDirectory);
        if (segmentDiscovery.error() != null) {
            return segmentDiscovery.error();
        }
        final List<Path> files = segmentDiscovery.files();
        long records = 0L;
        long maxLsn = 0L;
        long previousLsn = 0L;
        for (final Path file : files) {
            final byte[] bytes = readAll(file);
            long offset = 0L;
            while (offset < bytes.length) {
                if (bytes.length - offset < 4) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "Partial record length prefix.");
                }
                final int bodyLen = WalRuntime.readInt(bytes, (int) offset);
                if (bodyLen < MIN_RECORD_BODY_SIZE
                        || bodyLen > MAX_RECORD_BODY_SIZE) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "Invalid WAL record body length.");
                }
                if (offset + 4L + bodyLen > bytes.length) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "Truncated WAL record body.");
                }
                final int bodyOffset = (int) (offset + 4L);
                final int storedCrc = WalRuntime.readInt(bytes, bodyOffset);
                final int computedCrc = WalRuntime.computeCrc32(bytes,
                        bodyOffset + 4, bodyLen - 4);
                if (storedCrc != computedCrc) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "CRC mismatch.");
                }
                int p = bodyOffset + 4;
                final long lsn = WalRuntime.readLong(bytes, p);
                p += 8;
                final byte opCode = bytes[p++];
                if (opCode != 1 && opCode != 2) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "Invalid WAL operation code.");
                }
                final int keyLen = WalRuntime.readInt(bytes, p);
                p += 4;
                final int valueLen = WalRuntime.readInt(bytes, p);
                p += 4;
                if (keyLen <= 0 || valueLen < 0
                        || p + keyLen + valueLen != bodyOffset + bodyLen) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "Invalid key/value frame lengths.");
                }
                if (opCode == 2 && valueLen != 0) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "DELETE record must have zero value length.");
                }
                if (lsn <= 0L || (previousLsn > 0L && lsn <= previousLsn)) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset,
                            "Non-monotonic or invalid LSN.");
                }
                previousLsn = lsn;
                maxLsn = Math.max(maxLsn, lsn);
                records++;
                offset += 4L + bodyLen;
            }
        }
        if (checkpointValidation.checkpointLsn() != null
                && checkpointValidation.checkpointLsn().longValue() > maxLsn) {
            return new VerifyResult(false, files.size(), records, maxLsn,
                    CHECKPOINT_FILE, -1L,
                    "Checkpoint LSN is ahead of max WAL LSN.");
        }
        return new VerifyResult(true, files.size(), records, maxLsn, null, -1L,
                null);
    }

    static void dump(final Path walDirectory) {
        dump(walDirectory, System.out);
    }

    static void dump(final Path walDirectory, final PrintStream out) {
        dump(walDirectory, out, OutputMode.TEXT);
    }

    static void dump(final Path walDirectory, final PrintStream out,
            final OutputMode outputMode) {
        final SegmentDiscovery segmentDiscovery = discoverSegments(walDirectory);
        if (segmentDiscovery.error() != null) {
            throw new IndexException(segmentDiscovery.error().errorMessage());
        }
        final List<Path> files = segmentDiscovery.files();
        for (final Path file : files) {
            final byte[] bytes = readAll(file);
            long offset = 0L;
            long records = 0L;
            long firstLsn = 0L;
            long lastLsn = 0L;
            boolean invalidTail = false;
            String invalidReason = null;
            long invalidOffset = -1L;
            while (offset < bytes.length) {
                if (bytes.length - offset < 4L) {
                    invalidTail = true;
                    invalidReason = "Partial record length prefix.";
                    invalidOffset = offset;
                    break;
                }
                final int bodyLen = WalRuntime.readInt(bytes, (int) offset);
                if (bodyLen < MIN_RECORD_BODY_SIZE
                        || bodyLen > MAX_RECORD_BODY_SIZE) {
                    invalidTail = true;
                    invalidReason = "Invalid WAL record body length.";
                    invalidOffset = offset;
                    break;
                }
                if (offset + 4L + bodyLen > bytes.length) {
                    invalidTail = true;
                    invalidReason = "Truncated WAL record body.";
                    invalidOffset = offset;
                    break;
                }
                final int bodyOffset = (int) (offset + 4L);
                final int storedCrc = WalRuntime.readInt(bytes, bodyOffset);
                final int computedCrc = WalRuntime.computeCrc32(bytes,
                        bodyOffset + 4, bodyLen - 4);
                if (storedCrc != computedCrc) {
                    invalidTail = true;
                    invalidReason = "CRC mismatch.";
                    invalidOffset = offset;
                    break;
                }
                int p = bodyOffset + 4;
                final long lsn = WalRuntime.readLong(bytes, p);
                p += 8;
                final byte opCode = bytes[p++];
                final String operation = toOperationName(opCode);
                if (operation == null) {
                    invalidTail = true;
                    invalidReason = "Invalid WAL operation code.";
                    invalidOffset = offset;
                    break;
                }
                final int keyLen = WalRuntime.readInt(bytes, p);
                p += 4;
                final int valueLen = WalRuntime.readInt(bytes, p);
                p += 4;
                if (keyLen <= 0 || valueLen < 0
                        || p + keyLen + valueLen != bodyOffset + bodyLen) {
                    invalidTail = true;
                    invalidReason = "Invalid key/value frame lengths.";
                    invalidOffset = offset;
                    break;
                }
                if ("DELETE".equals(operation) && valueLen != 0) {
                    invalidTail = true;
                    invalidReason = "DELETE record must have zero value length.";
                    invalidOffset = offset;
                    break;
                }
                if (outputMode == OutputMode.JSON) {
                    out.println(jsonRecord(file.getFileName().toString(), offset,
                            lsn, operation, keyLen, valueLen, bodyLen));
                } else {
                    out.println("record file=" + file.getFileName() + " offset="
                            + offset + " lsn=" + lsn + " op=" + operation
                            + " keyLen=" + keyLen + " valueLen=" + valueLen
                            + " bodyLen=" + bodyLen);
                }
                if (records == 0L) {
                    firstLsn = lsn;
                }
                lastLsn = lsn;
                records++;
                offset += 4L + bodyLen;
            }
            if (invalidTail) {
                if (outputMode == OutputMode.JSON) {
                    out.println(jsonInvalidTail(file.getFileName().toString(),
                            invalidOffset, invalidReason));
                } else {
                    out.println("invalid file=" + file.getFileName()
                            + " offset=" + invalidOffset + " reason="
                            + invalidReason);
                }
            }
            if (outputMode == OutputMode.JSON) {
                out.println(jsonSummary(file.getFileName().toString(),
                        bytes.length, records, firstLsn, lastLsn));
            } else {
                out.println("summary file=" + file.getFileName() + " size="
                        + bytes.length + " records=" + records + " firstLsn="
                        + firstLsn + " lastLsn=" + lastLsn);
            }
        }
    }

    private static SegmentDiscovery discoverSegments(final Path walDirectory) {
        final List<Path> candidates;
        try (Stream<Path> listing = Files.list(walDirectory)) {
            candidates = listing
                    .filter(path -> path.getFileName().toString()
                            .endsWith(SEGMENT_SUFFIX))
                    .sorted(Comparator.comparing(Path::getFileName)).toList();
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to list WAL directory '%s'.",
                            walDirectory),
                    e);
        }

        final List<SegmentFile> discovered = new ArrayList<>(candidates.size());
        final Set<Long> uniqueBaseLsns = new HashSet<>();
        for (final Path file : candidates) {
            final String fileName = file.getFileName().toString();
            final long baseLsn = parseSegmentBaseLsn(fileName);
            if (baseLsn < 0L) {
                return new SegmentDiscovery(List.of(),
                        new VerifyResult(false, 0, 0L, 0L, fileName, -1L,
                                "Invalid WAL segment file name."));
            }
            if (!uniqueBaseLsns.add(baseLsn)) {
                return new SegmentDiscovery(List.of(),
                        new VerifyResult(false, 0, 0L, 0L, fileName, -1L,
                                "Duplicate WAL segment base LSN."));
            }
            discovered.add(new SegmentFile(file, baseLsn));
        }
        discovered.sort(Comparator.comparingLong(SegmentFile::baseLsn));
        final List<Path> files = discovered.stream().map(SegmentFile::path)
                .toList();
        return new SegmentDiscovery(files, null);
    }

    private static long parseSegmentBaseLsn(final String fileName) {
        final String raw = fileName.substring(0,
                fileName.length() - SEGMENT_SUFFIX.length());
        if (raw.isBlank()) {
            return -1L;
        }
        for (int i = 0; i < raw.length(); i++) {
            if (!Character.isDigit(raw.charAt(i))) {
                return -1L;
            }
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            return -1L;
        }
    }

    private static String toOperationName(final byte opCode) {
        if (opCode == 1) {
            return "PUT";
        }
        if (opCode == 2) {
            return "DELETE";
        }
        return null;
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

    private static OutputMode parseOutputMode(final String[] args,
            final PrintStream out) {
        if (args.length == 2) {
            return OutputMode.TEXT;
        }
        if (args.length == 3 && "--json".equals(args[2])) {
            return OutputMode.JSON;
        }
        printUsage(out);
        return null;
    }

    private static void printVerifyResult(final VerifyResult result,
            final PrintStream out, final OutputMode outputMode) {
        if (outputMode == OutputMode.JSON) {
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
                + "\"file\":" + toJsonString(fileName) + ","
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
                + "\"file\":" + toJsonString(fileName) + ","
                + "\"offset\":" + invalidOffset + ","
                + "\"reason\":" + toJsonString(reason) + "}";
    }

    private static String jsonSummary(final String fileName, final long size,
            final long records, final long firstLsn, final long lastLsn) {
        return "{\"type\":\"summary\","
                + "\"file\":" + toJsonString(fileName) + ","
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
                    escaped.append(String.format("\\u%04x", (int) ch));
                } else {
                    escaped.append(ch);
                }
            }
            }
        }
        return escaped.toString();
    }

    private static VerifyResult verifyFormatMetadata(final Path walDirectory) {
        final Path formatFile = walDirectory.resolve(FORMAT_FILE);
        if (!Files.exists(formatFile)) {
            return new VerifyResult(false, 0, 0L, 0L, FORMAT_FILE, -1L,
                    "Missing WAL format metadata.");
        }
        final byte[] bytes = readAll(formatFile);
        final String text = new String(bytes, StandardCharsets.US_ASCII).trim();
        if (text.isEmpty()) {
            return new VerifyResult(false, 0, 0L, 0L, FORMAT_FILE, -1L,
                    "Empty WAL format metadata.");
        }
        Integer version = null;
        Integer checksum = null;
        for (final String line : text.split("\\R")) {
            final String[] parts = line.split("=", 2);
            if (parts.length != 2) {
                continue;
            }
            final String key = parts[0].trim();
            final String value = parts[1].trim();
            try {
                if ("version".equals(key)) {
                    version = Integer.valueOf(value);
                } else if ("checksum".equals(key)) {
                    checksum = Integer.valueOf(value);
                }
            } catch (RuntimeException e) {
                return new VerifyResult(false, 0, 0L, 0L, FORMAT_FILE, -1L,
                        "Invalid numeric value in WAL format metadata.");
            }
        }
        if (version == null || checksum == null) {
            return new VerifyResult(false, 0, 0L, 0L, FORMAT_FILE, -1L,
                    "Missing version/checksum in WAL format metadata.");
        }
        final byte[] payload = ("version=" + FORMAT_VERSION + "\n")
                .getBytes(StandardCharsets.US_ASCII);
        final int expectedChecksum = WalRuntime.computeCrc32(payload, 0,
                payload.length);
        if (version.intValue() != FORMAT_VERSION
                || checksum.intValue() != expectedChecksum) {
            return new VerifyResult(false, 0, 0L, 0L, FORMAT_FILE, -1L,
                    "Unsupported or corrupted WAL format metadata.");
        }
        return null;
    }

    private static CheckpointValidation readCheckpointMetadata(
            final Path walDirectory) {
        final Path checkpointFile = walDirectory.resolve(CHECKPOINT_FILE);
        if (!Files.exists(checkpointFile)) {
            return new CheckpointValidation(null, null);
        }
        final String text = new String(readAll(checkpointFile),
                StandardCharsets.US_ASCII).trim();
        if (text.isEmpty()) {
            return new CheckpointValidation(null, null);
        }
        try {
            final long checkpointLsn = Long.parseLong(text);
            if (checkpointLsn < 0L) {
                return new CheckpointValidation(null,
                        new VerifyResult(false, 0, 0L, 0L, CHECKPOINT_FILE,
                                -1L, "Negative checkpoint LSN."));
            }
            return new CheckpointValidation(Long.valueOf(checkpointLsn), null);
        } catch (RuntimeException e) {
            return new CheckpointValidation(null, new VerifyResult(false, 0, 0L,
                    0L, CHECKPOINT_FILE, -1L, "Invalid checkpoint metadata."));
        }
    }

    record VerifyResult(boolean ok, int fileCount, long recordCount, long maxLsn,
            String errorFile, long errorOffset, String errorMessage) {
    }

    record SegmentFile(Path path, long baseLsn) {
    }

    record SegmentDiscovery(List<Path> files, VerifyResult error) {
    }

    record CheckpointValidation(Long checkpointLsn, VerifyResult error) {
    }
}
