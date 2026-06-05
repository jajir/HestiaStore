package org.hestiastore.index.segmentindex.wal;

/**
 * Immutable WAL runtime metrics snapshot.
 */
public record WalStats(long appendCount, long appendBytes, long syncCount,
        long syncFailureCount, long corruptionCount, long truncationCount,
        long retainedBytes, int segmentCount, long durableLsn,
        long checkpointLsn, long pendingSyncBytes, long syncTotalNanos,
        long syncMaxNanos, long syncBatchBytesTotal,
        long syncBatchBytesMax) {

    /**
     * Returns an empty WAL metrics snapshot for disabled WAL coordination.
     *
     * @return empty WAL metrics snapshot
     */
    public static WalStats empty() {
        return new WalStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0L, 0L, 0L, 0L,
                0L, 0L, 0L);
    }
}
