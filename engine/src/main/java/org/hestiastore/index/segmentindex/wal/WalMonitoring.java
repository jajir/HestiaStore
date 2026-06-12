package org.hestiastore.index.segmentindex.wal;

import org.hestiastore.index.Vldtn;

/**
 * WAL-owned runtime monitoring data holder.
 */
public final class WalMonitoring {

    private final long appendCount;
    private final long appendBytes;
    private final long syncCount;
    private final long syncFailureCount;
    private final long corruptionCount;
    private final long truncationCount;
    private final long retainedBytes;
    private final int segmentCount;
    private final long durableLsn;
    private final long checkpointLsn;
    private final long pendingSyncBytes;
    private final long syncTotalNanos;
    private final long syncMaxNanos;
    private final long syncBatchBytesTotal;
    private final long syncBatchBytesMax;

    /**
     * Creates WAL monitoring data.
     *
     * @param appendCount append count
     * @param appendBytes append byte count
     * @param syncCount sync count
     * @param syncFailureCount sync failure count
     * @param corruptionCount corruption count
     * @param truncationCount truncation count
     * @param retainedBytes retained WAL bytes
     * @param segmentCount WAL segment count
     * @param durableLsn durable LSN
     * @param checkpointLsn checkpoint LSN
     * @param pendingSyncBytes pending sync bytes
     * @param syncTotalNanos total sync nanos
     * @param syncMaxNanos max sync nanos
     * @param syncBatchBytesTotal total sync batch bytes
     * @param syncBatchBytesMax max sync batch bytes
     */
    @SuppressWarnings("java:S107")
    public WalMonitoring(final long appendCount, final long appendBytes,
            final long syncCount, final long syncFailureCount,
            final long corruptionCount, final long truncationCount,
            final long retainedBytes, final int segmentCount,
            final long durableLsn, final long checkpointLsn,
            final long pendingSyncBytes, final long syncTotalNanos,
            final long syncMaxNanos, final long syncBatchBytesTotal,
            final long syncBatchBytesMax) {
        this.appendCount = Vldtn.requireGreaterThanOrEqualToZero(appendCount,
                "appendCount");
        this.appendBytes = Vldtn.requireGreaterThanOrEqualToZero(appendBytes,
                "appendBytes");
        this.syncCount = Vldtn.requireGreaterThanOrEqualToZero(syncCount,
                "syncCount");
        this.syncFailureCount = Vldtn.requireGreaterThanOrEqualToZero(
                syncFailureCount, "syncFailureCount");
        this.corruptionCount = Vldtn.requireGreaterThanOrEqualToZero(
                corruptionCount, "corruptionCount");
        this.truncationCount = Vldtn.requireGreaterThanOrEqualToZero(
                truncationCount, "truncationCount");
        this.retainedBytes = Vldtn.requireGreaterThanOrEqualToZero(
                retainedBytes, "retainedBytes");
        this.segmentCount = Vldtn.requireGreaterThanOrEqualToZero(segmentCount,
                "segmentCount");
        this.durableLsn = Vldtn.requireGreaterThanOrEqualToZero(durableLsn,
                "durableLsn");
        this.checkpointLsn = Vldtn.requireGreaterThanOrEqualToZero(
                checkpointLsn, "checkpointLsn");
        this.pendingSyncBytes = Vldtn.requireGreaterThanOrEqualToZero(
                pendingSyncBytes, "pendingSyncBytes");
        this.syncTotalNanos = Vldtn.requireGreaterThanOrEqualToZero(
                syncTotalNanos, "syncTotalNanos");
        this.syncMaxNanos = Vldtn.requireGreaterThanOrEqualToZero(syncMaxNanos,
                "syncMaxNanos");
        this.syncBatchBytesTotal = Vldtn.requireGreaterThanOrEqualToZero(
                syncBatchBytesTotal, "syncBatchBytesTotal");
        this.syncBatchBytesMax = Vldtn.requireGreaterThanOrEqualToZero(
                syncBatchBytesMax, "syncBatchBytesMax");
    }

    /**
     * Returns empty WAL monitoring data for disabled WAL coordination.
     *
     * @return empty WAL monitoring data
     */
    public static WalMonitoring empty() {
        return new WalMonitoring(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0L, 0L,
                0L, 0L, 0L, 0L, 0L);
    }

    /**
     * Returns append count.
     *
     * @return append count
     */
    public long appendCount() {
        return appendCount;
    }

    /**
     * Returns append byte count.
     *
     * @return append byte count
     */
    public long appendBytes() {
        return appendBytes;
    }

    /**
     * Returns sync count.
     *
     * @return sync count
     */
    public long syncCount() {
        return syncCount;
    }

    /**
     * Returns sync failure count.
     *
     * @return sync failure count
     */
    public long syncFailureCount() {
        return syncFailureCount;
    }

    /**
     * Returns corruption count.
     *
     * @return corruption count
     */
    public long corruptionCount() {
        return corruptionCount;
    }

    /**
     * Returns truncation count.
     *
     * @return truncation count
     */
    public long truncationCount() {
        return truncationCount;
    }

    /**
     * Returns retained WAL bytes.
     *
     * @return retained WAL bytes
     */
    public long retainedBytes() {
        return retainedBytes;
    }

    /**
     * Returns WAL segment count.
     *
     * @return WAL segment count
     */
    public int segmentCount() {
        return segmentCount;
    }

    /**
     * Returns durable LSN.
     *
     * @return durable LSN
     */
    public long durableLsn() {
        return durableLsn;
    }

    /**
     * Returns checkpoint LSN.
     *
     * @return checkpoint LSN
     */
    public long checkpointLsn() {
        return checkpointLsn;
    }

    /**
     * Returns pending sync bytes.
     *
     * @return pending sync bytes
     */
    public long pendingSyncBytes() {
        return pendingSyncBytes;
    }

    /**
     * Returns total sync nanos.
     *
     * @return total sync nanos
     */
    public long syncTotalNanos() {
        return syncTotalNanos;
    }

    /**
     * Returns max sync nanos.
     *
     * @return max sync nanos
     */
    public long syncMaxNanos() {
        return syncMaxNanos;
    }

    /**
     * Returns total sync batch bytes.
     *
     * @return total sync batch bytes
     */
    public long syncBatchBytesTotal() {
        return syncBatchBytesTotal;
    }

    /**
     * Returns max sync batch bytes.
     *
     * @return max sync batch bytes
     */
    public long syncBatchBytesMax() {
        return syncBatchBytesMax;
    }
}
