package org.hestiastore.index.segmentindex.wal;

final class WalSegmentDescriptor {

    private final String name;
    private final long baseLsn;
    private long sizeBytes;
    private long maxLsn;

    WalSegmentDescriptor(final String name, final long baseLsn,
            final long sizeBytes, final long maxLsn) {
        this.name = name;
        this.baseLsn = baseLsn;
        this.sizeBytes = sizeBytes;
        this.maxLsn = maxLsn;
    }

    String name() {
        return name;
    }

    long baseLsn() {
        return baseLsn;
    }

    long sizeBytes() {
        return sizeBytes;
    }

    long maxLsn() {
        return maxLsn;
    }

    void grow(final long bytes, final long lsn) {
        this.sizeBytes += bytes;
        if (lsn > this.maxLsn) {
            this.maxLsn = lsn;
        }
    }
}
