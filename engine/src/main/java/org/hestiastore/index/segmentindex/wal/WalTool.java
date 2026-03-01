package org.hestiastore.index.segmentindex.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;

/**
 * Command-line helper for WAL inspection and verification.
 */
public final class WalTool {

    private static final String SEGMENT_SUFFIX = ".wal";
    private static final int MIN_RECORD_BODY_SIZE = 4 + 8 + 1 + 4 + 4;
    private static final int MAX_RECORD_BODY_SIZE = 32 * 1024 * 1024;

    private WalTool() {
    }

    public static void main(final String[] args) {
        if (args == null || args.length < 2) {
            printUsage();
            return;
        }
        final String command = args[0];
        final Path walDirectory = Path.of(args[1]);
        try {
            if ("verify".equals(command)) {
                final VerifyResult result = verify(walDirectory);
                System.out.println("verify.ok=" + result.ok());
                System.out.println("verify.files=" + result.fileCount());
                System.out.println("verify.records=" + result.recordCount());
                System.out.println("verify.maxLsn=" + result.maxLsn());
                if (!result.ok()) {
                    System.out.println("verify.errorFile=" + result.errorFile());
                    System.out.println(
                            "verify.errorOffset=" + result.errorOffset());
                }
                return;
            }
            if ("dump".equals(command)) {
                dump(walDirectory);
                return;
            }
            printUsage();
        } catch (RuntimeException e) {
            System.err.println("WAL tool failed: " + e.getMessage());
            System.exit(1);
        }
    }

    static VerifyResult verify(final Path walDirectory) {
        final List<Path> files = listSegmentFiles(walDirectory);
        long records = 0L;
        long maxLsn = 0L;
        for (final Path file : files) {
            final byte[] bytes = readAll(file);
            long offset = 0L;
            long previousLsn = 0L;
            while (offset < bytes.length) {
                if (bytes.length - offset < 4) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                final int bodyLen = WalRuntime.readInt(bytes, (int) offset);
                if (bodyLen < MIN_RECORD_BODY_SIZE
                        || bodyLen > MAX_RECORD_BODY_SIZE) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                if (offset + 4L + bodyLen > bytes.length) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                final int bodyOffset = (int) (offset + 4L);
                final int storedCrc = WalRuntime.readInt(bytes, bodyOffset);
                final int computedCrc = WalRuntime.computeCrc32(bytes,
                        bodyOffset + 4, bodyLen - 4);
                if (storedCrc != computedCrc) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                int p = bodyOffset + 4;
                final long lsn = WalRuntime.readLong(bytes, p);
                p += 8;
                final byte opCode = bytes[p++];
                if (opCode != 1 && opCode != 2) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                final int keyLen = WalRuntime.readInt(bytes, p);
                p += 4;
                final int valueLen = WalRuntime.readInt(bytes, p);
                p += 4;
                if (keyLen <= 0 || valueLen < 0
                        || p + keyLen + valueLen != bodyOffset + bodyLen) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                if (opCode == 2 && valueLen != 0) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                if (lsn <= 0L || (previousLsn > 0L && lsn <= previousLsn)) {
                    return new VerifyResult(false, files.size(), records, maxLsn,
                            file.getFileName().toString(), offset);
                }
                previousLsn = lsn;
                maxLsn = Math.max(maxLsn, lsn);
                records++;
                offset += 4L + bodyLen;
            }
        }
        return new VerifyResult(true, files.size(), records, maxLsn, null, -1L);
    }

    static void dump(final Path walDirectory) {
        final List<Path> files = listSegmentFiles(walDirectory);
        for (final Path file : files) {
            final byte[] bytes = readAll(file);
            long offset = 0L;
            long records = 0L;
            long firstLsn = 0L;
            long lastLsn = 0L;
            while (offset + 4L <= bytes.length) {
                final int bodyLen = WalRuntime.readInt(bytes, (int) offset);
                if (bodyLen < MIN_RECORD_BODY_SIZE
                        || bodyLen > MAX_RECORD_BODY_SIZE
                        || offset + 4L + bodyLen > bytes.length) {
                    break;
                }
                final int bodyOffset = (int) (offset + 4L);
                final long lsn = WalRuntime.readLong(bytes, bodyOffset + 4);
                if (records == 0L) {
                    firstLsn = lsn;
                }
                lastLsn = lsn;
                records++;
                offset += 4L + bodyLen;
            }
            System.out.println("file=" + file.getFileName() + " size="
                    + bytes.length + " records=" + records + " firstLsn="
                    + firstLsn + " lastLsn=" + lastLsn);
        }
    }

    private static List<Path> listSegmentFiles(final Path walDirectory) {
        try (Stream<Path> listing = Files.list(walDirectory)) {
            return listing
                    .filter(path -> path.getFileName().toString()
                            .endsWith(SEGMENT_SUFFIX))
                    .sorted(Comparator.comparing(Path::getFileName)).toList();
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to list WAL directory '%s'.",
                            walDirectory),
                    e);
        }
    }

    private static byte[] readAll(final Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new IndexException(
                    String.format("Unable to read WAL file '%s'.", file), e);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: WalTool <verify|dump> <walDirectoryPath>");
    }

    record VerifyResult(boolean ok, int fileCount, long recordCount, long maxLsn,
            String errorFile, long errorOffset) {
    }
}
