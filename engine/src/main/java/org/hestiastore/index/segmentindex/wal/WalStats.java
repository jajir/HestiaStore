package org.hestiastore.index.segmentindex.wal;

/**
 * Immutable WAL runtime metrics snapshot.
 */
public record WalStats(long appendCount, long appendBytes, long syncCount,
        long syncFailureCount, long corruptionCount, long truncationCount,
        long retainedBytes, int segmentCount, long durableLsn,
        long checkpointLsn, long pendingSyncBytes) {
}
