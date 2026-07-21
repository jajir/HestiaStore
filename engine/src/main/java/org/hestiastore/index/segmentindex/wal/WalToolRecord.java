package org.hestiastore.index.segmentindex.wal;

/**
 * Record metadata emitted by the WAL command-line tool.
 */
final class WalToolRecord {

    private final long offset;
    private final long lsn;
    private final String operation;
    private final int keyLen;
    private final int valueLen;
    private final int bodyLen;

    WalToolRecord(final long offset, final long lsn, final String operation,
            final int keyLen, final int valueLen, final int bodyLen) {
        this.offset = offset;
        this.lsn = lsn;
        this.operation = operation;
        this.keyLen = keyLen;
        this.valueLen = valueLen;
        this.bodyLen = bodyLen;
    }

    long offset() {
        return offset;
    }

    long lsn() {
        return lsn;
    }

    String operation() {
        return operation;
    }

    int keyLen() {
        return keyLen;
    }

    int valueLen() {
        return valueLen;
    }

    int bodyLen() {
        return bodyLen;
    }
}
