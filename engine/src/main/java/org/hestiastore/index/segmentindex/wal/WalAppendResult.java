package org.hestiastore.index.segmentindex.wal;

/**
 * WAL append metadata needed by durability policy after a record is written.
 */
final class WalAppendResult {

    private final long lsn;
    private final int recordBytes;
    private final String segmentName;

    WalAppendResult(final long lsn, final int recordBytes,
            final String segmentName) {
        this.lsn = lsn;
        this.recordBytes = recordBytes;
        this.segmentName = segmentName;
    }

    long lsn() {
        return lsn;
    }

    int recordBytes() {
        return recordBytes;
    }

    String segmentName() {
        return segmentName;
    }
}
