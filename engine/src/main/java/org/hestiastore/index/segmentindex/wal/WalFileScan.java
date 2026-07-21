package org.hestiastore.index.segmentindex.wal;

import java.util.List;

/**
 * Result of scanning one WAL file for the command-line tool.
 */
final class WalFileScan {

    private final String fileName;
    private final long size;
    private final List<WalToolRecord> records;
    private final long invalidOffset;
    private final String invalidReason;

    private WalFileScan(final String fileName, final long size,
            final List<WalToolRecord> records, final long invalidOffset,
            final String invalidReason) {
        this.fileName = fileName;
        this.size = size;
        this.records = List.copyOf(records);
        this.invalidOffset = invalidOffset;
        this.invalidReason = invalidReason;
    }

    static WalFileScan valid(final String fileName, final long size,
            final List<WalToolRecord> records) {
        return new WalFileScan(fileName, size, records, -1L, null);
    }

    static WalFileScan invalid(final String fileName, final long size,
            final List<WalToolRecord> records, final long offset,
            final String reason) {
        return new WalFileScan(fileName, size, records, offset, reason);
    }

    String fileName() {
        return fileName;
    }

    long size() {
        return size;
    }

    List<WalToolRecord> records() {
        return records;
    }

    long recordCount() {
        return records.size();
    }

    long firstLsn() {
        return records.isEmpty() ? 0L : records.get(0).lsn();
    }

    long lastLsn() {
        return records.isEmpty() ? 0L : records.get(records.size() - 1).lsn();
    }

    boolean hasInvalidTail() {
        return invalidReason != null;
    }

    long invalidOffset() {
        return invalidOffset;
    }

    String invalidReason() {
        return invalidReason;
    }
}
