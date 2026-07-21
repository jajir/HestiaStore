package org.hestiastore.index.segmentindex.wal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.hestiastore.index.IndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns WAL metadata persistence, validation, and segment discovery.
 */
final class WalMetadataCatalog {

    static final String WAL_DIRECTORY = "wal";

    static final String FORMAT_FILE = "format.meta";
    static final String CHECKPOINT_FILE = "checkpoint.meta";
    static final String CHECKPOINT_FILE_TMP = "checkpoint.meta.tmp";
    static final String FORMAT_FILE_TMP = "format.meta.tmp";
    static final String CHECKPOINT_KEY_LSN = "lsn";
    static final String CHECKPOINT_KEY_CHECKSUM = "checksum";
    static final String SEGMENT_SUFFIX = ".wal";
    private static final int SEGMENT_FILE_DIGITS = 20;
    private static final String SEGMENT_FILE_FORMAT = "%020d" + SEGMENT_SUFFIX;
    private static final int FORMAT_VERSION = 1;
    private static final Logger LOGGER = LoggerFactory
            .getLogger(WalMetadataCatalog.class);

    private final WalStorage storage;

    WalMetadataCatalog(final WalStorage storage) {
        this.storage = storage;
    }

    /**
     * Creates or validates the WAL format marker before WAL access.
     */
    void ensureFormatMarker() {
        final byte[] payload = formatPayload();
        reconcileFormatMetadataTempFile(payload);
        if (!storage.exists(FORMAT_FILE)) {
            writeFormatMarker(payload);
            return;
        }
        final byte[] existing = storage.readAll(FORMAT_FILE);
        validateFormatMetadata(existing);
    }

    void writeCheckpointLsnAtomic(final long checkpointLsn) {
        final byte[] data = checkpointMetadata(checkpointLsn);
        storage.overwrite(CHECKPOINT_FILE_TMP, data, 0, data.length);
        storage.sync(CHECKPOINT_FILE_TMP);
        storage.rename(CHECKPOINT_FILE_TMP, CHECKPOINT_FILE);
        storage.sync(CHECKPOINT_FILE);
        storage.syncMetadata();
    }

    String toSegmentFileName(final long baseLsn) {
        if (baseLsn < 0L) {
            throw new IllegalArgumentException(
                    "WAL segment base LSN must be non-negative.");
        }
        return String.format(Locale.ROOT, SEGMENT_FILE_FORMAT, baseLsn);
    }

    /**
     * Discovers and validates WAL segment entries for recovery.
     *
     * @return ordered segment descriptors
     */
    List<WalSegmentDescriptor> discoverSegmentsStrict() {
        final List<String> segmentNames = storage.listFileNames().stream()
                .filter(name -> name.endsWith(SEGMENT_SUFFIX)).toList();
        final List<WalSegmentDescriptor> discovered = new ArrayList<>(
                segmentNames.size());
        final Set<Long> uniqueBaseLsns = new HashSet<>();
        for (final String name : segmentNames) {
            final long baseLsn = parseSegmentBaseLsn(name);
            if (baseLsn < 0L) {
                throw new IndexException(String.format(
                        "Invalid WAL segment name '%s'.", name));
            }
            if (!storage.exists(name)) {
                throw new IndexException(String.format(
                        "WAL segment entry '%s' is not a regular file.", name));
            }
            if (!uniqueBaseLsns.add(baseLsn)) {
                throw new IndexException(String.format(
                        "Duplicate WAL segment base LSN detected for '%s'.",
                        baseLsn));
            }
            discovered.add(new WalSegmentDescriptor(name, baseLsn, 0L, baseLsn));
        }
        discovered.sort(Comparator.comparingLong(WalSegmentDescriptor::baseLsn));
        return discovered;
    }

    static long parseSegmentBaseLsn(final String fileName) {
        if (fileName == null || !fileName.endsWith(SEGMENT_SUFFIX)
                || fileName.length() <= SEGMENT_SUFFIX.length()) {
            return -1L;
        }
        final String raw = fileName.substring(0,
                fileName.length() - SEGMENT_SUFFIX.length());
        if (raw.length() != SEGMENT_FILE_DIGITS) {
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

    /**
     * Reconciles checkpoint metadata and returns the persisted checkpoint LSN.
     *
     * @return persisted checkpoint LSN, or zero when absent
     */
    long readCheckpointLsn() {
        reconcileCheckpointMetadataTempFile();
        if (!storage.exists(CHECKPOINT_FILE)) {
            return 0L;
        }
        final byte[] data = storage.readAll(CHECKPOINT_FILE);
        if (data.length == 0) {
            return 0L;
        }
        return parseCheckpointMetadata(data);
    }

    private void reconcileCheckpointMetadataTempFile() {
        if (!storage.exists(CHECKPOINT_FILE_TMP)) {
            return;
        }
        if (storage.exists(CHECKPOINT_FILE)) {
            storage.delete(CHECKPOINT_FILE_TMP);
            storage.syncMetadata();
            return;
        }
        final byte[] temporary = storage.readAll(CHECKPOINT_FILE_TMP);
        if (temporary.length == 0) {
            storage.delete(CHECKPOINT_FILE_TMP);
            storage.syncMetadata();
            return;
        }
        try {
            final long parsed = parseCheckpointMetadata(temporary);
            storage.rename(CHECKPOINT_FILE_TMP, CHECKPOINT_FILE);
            storage.sync(CHECKPOINT_FILE);
            storage.syncMetadata();
            LOGGER.info(
                    "event=wal_checkpoint_metadata_tmp_recovered checkpointLsn={}",
                    parsed);
        } catch (RuntimeException ex) {
            LOGGER.warn(
                    "event=wal_checkpoint_metadata_tmp_dropped reason=invalid error={}",
                    ex.getMessage());
            storage.delete(CHECKPOINT_FILE_TMP);
            storage.syncMetadata();
        }
    }

    static long parseCheckpointMetadata(final byte[] data) {
        final String text = new String(data, StandardCharsets.US_ASCII).trim();
        if (text.isEmpty()) {
            return 0L;
        }
        if (!text.contains("=")) {
            return parseLegacyCheckpointMetadata(text);
        }
        return parseChecksummedCheckpointMetadata(text);
    }

    private static long parseLegacyCheckpointMetadata(final String text) {
        final long parsed;
        try {
            parsed = Long.parseLong(text);
        } catch (RuntimeException ex) {
            throw new IndexException("Invalid WAL checkpoint metadata.", ex);
        }
        if (parsed < 0L) {
            throw new IndexException(String.format(
                    "Invalid WAL checkpoint metadata: negative LSN '%s'.",
                    parsed));
        }
        return parsed;
    }

    private static long parseChecksummedCheckpointMetadata(final String text) {
        Long lsn = null;
        Integer checksum = null;
        for (final String line : text.split("\\R")) {
            final String[] parts = line.split("=", 2);
            if (parts.length != 2) {
                continue;
            }
            final String key = parts[0].trim();
            final String value = parts[1].trim();
            try {
                if (CHECKPOINT_KEY_LSN.equals(key)) {
                    lsn = Long.valueOf(value);
                } else if (CHECKPOINT_KEY_CHECKSUM.equals(key)) {
                    checksum = Integer.valueOf(value);
                }
            } catch (RuntimeException ex) {
                throw new IndexException("Invalid WAL checkpoint metadata.",
                        ex);
            }
        }
        if (lsn == null || checksum == null) {
            throw new IndexException("Invalid WAL checkpoint metadata.");
        }
        if (lsn.longValue() < 0L) {
            throw new IndexException(String.format(
                    "Invalid WAL checkpoint metadata: negative LSN '%s'.",
                    lsn));
        }
        final byte[] payload = checkpointPayload(lsn.longValue());
        final int expectedChecksum = WalRecordCodec.computeCrc32(payload, 0,
                payload.length);
        if (checksum.intValue() != expectedChecksum) {
            throw new IndexException(String.format(
                    "Invalid WAL checkpoint metadata checksum: expected=%s actual=%s.",
                    expectedChecksum, checksum));
        }
        return lsn.longValue();
    }

    private byte[] checkpointMetadata(final long checkpointLsn) {
        final byte[] payload = checkpointPayload(checkpointLsn);
        final int checksum = WalRecordCodec.computeCrc32(payload, 0,
                payload.length);
        final String text = CHECKPOINT_KEY_LSN + "=" + checkpointLsn + "\n"
                + CHECKPOINT_KEY_CHECKSUM + "=" + checksum + "\n";
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] checkpointPayload(final long checkpointLsn) {
        return (CHECKPOINT_KEY_LSN + "=" + checkpointLsn + "\n")
                .getBytes(StandardCharsets.US_ASCII);
    }

    private void reconcileFormatMetadataTempFile(final byte[] payload) {
        if (!storage.exists(FORMAT_FILE_TMP)) {
            return;
        }
        if (storage.exists(FORMAT_FILE)) {
            storage.delete(FORMAT_FILE_TMP);
            storage.syncMetadata();
            return;
        }
        final int expectedChecksum = WalRecordCodec.computeCrc32(payload, 0,
                payload.length);
        try {
            final byte[] temporary = storage.readAll(FORMAT_FILE_TMP);
            final FormatMeta meta = parseFormatMeta(temporary);
            if (meta.version() == FORMAT_VERSION
                    && meta.checksum() == expectedChecksum) {
                storage.rename(FORMAT_FILE_TMP, FORMAT_FILE);
                storage.sync(FORMAT_FILE);
                storage.syncMetadata();
                LOGGER.info(
                        "event=wal_format_metadata_tmp_recovered version={} checksum={}",
                        meta.version(), meta.checksum());
                return;
            }
            LOGGER.warn(
                    "event=wal_format_metadata_tmp_dropped reason=unsupported version={} checksum={} expectedVersion={} expectedChecksum={}",
                    meta.version(), meta.checksum(), FORMAT_VERSION,
                    expectedChecksum);
        } catch (RuntimeException ex) {
            LOGGER.warn(
                    "event=wal_format_metadata_tmp_dropped reason=invalid error={}",
                    ex.getMessage());
        }
        storage.delete(FORMAT_FILE_TMP);
        storage.syncMetadata();
    }

    private void writeFormatMarker(final byte[] payload) {
        final int checksum = WalRecordCodec.computeCrc32(payload, 0,
                payload.length);
        final byte[] data = ("version=" + FORMAT_VERSION + "\nchecksum="
                + checksum + "\n").getBytes(StandardCharsets.US_ASCII);
        storage.overwrite(FORMAT_FILE_TMP, data, 0, data.length);
        storage.sync(FORMAT_FILE_TMP);
        storage.rename(FORMAT_FILE_TMP, FORMAT_FILE);
        storage.sync(FORMAT_FILE);
        storage.syncMetadata();
    }

    private static byte[] formatPayload() {
        return ("version=" + FORMAT_VERSION + "\n")
                .getBytes(StandardCharsets.US_ASCII);
    }

    static void validateFormatMetadata(final byte[] bytes) {
        final byte[] payload = formatPayload();
        final FormatMeta meta = parseFormatMeta(bytes);
        final int expectedChecksum = WalRecordCodec.computeCrc32(payload, 0,
                payload.length);
        if (meta.version() != FORMAT_VERSION
                || meta.checksum() != expectedChecksum) {
            throw new IndexException(
                    "Unsupported or corrupted WAL format metadata.");
        }
    }

    private static FormatMeta parseFormatMeta(final byte[] bytes) {
        final String text = new String(bytes, StandardCharsets.US_ASCII).trim();
        if (text.isEmpty()) {
            throw new IndexException("WAL format metadata is empty.");
        }
        Integer version = null;
        Integer checksum = null;
        final String[] lines = text.split("\\R");
        for (final String line : lines) {
            final String[] parts = line.split("=", 2);
            if (parts.length != 2) {
                continue;
            }
            final String key = parts[0].trim();
            final String value = parts[1].trim();
            if ("version".equals(key)) {
                version = parseFormatInt(key, value);
            } else if (CHECKPOINT_KEY_CHECKSUM.equals(key)) {
                checksum = parseFormatInt(key, value);
            }
        }
        if (version == null || checksum == null) {
            throw new IndexException("Invalid WAL format metadata.");
        }
        return new FormatMeta(version.intValue(), checksum.intValue());
    }

    private static int parseFormatInt(final String key, final String value) {
        try {
            return Integer.parseInt(value);
        } catch (RuntimeException ex) {
            throw new IndexException(String.format(
                    "Invalid WAL format metadata value for key '%s': '%s'.",
                    key, value), ex);
        }
    }

    private static final class FormatMeta {
        private final int version;
        private final int checksum;

        FormatMeta(final int version, final int checksum) {
            this.version = version;
            this.checksum = checksum;
        }

        int version() {
            return version;
        }

        int checksum() {
            return checksum;
        }
    }
}
