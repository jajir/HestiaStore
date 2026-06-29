package org.hestiastore.index.segmentindex.wal;

final class WalRecoveryOutcome {

    private final long checkpointLsn;
    private final long lastReplayedLsn;
    private final long maxLsn;
    private final boolean truncatedTail;

    WalRecoveryOutcome(final long checkpointLsn, final long lastReplayedLsn,
            final long maxLsn, final boolean truncatedTail) {
        this.checkpointLsn = checkpointLsn;
        this.lastReplayedLsn = lastReplayedLsn;
        this.maxLsn = maxLsn;
        this.truncatedTail = truncatedTail;
    }

    long checkpointLsn() {
        return checkpointLsn;
    }

    long lastReplayedLsn() {
        return lastReplayedLsn;
    }

    long maxLsn() {
        return maxLsn;
    }

    boolean truncatedTail() {
        return truncatedTail;
    }
}
