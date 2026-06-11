package org.hestiastore.index.segmentindex.runtimemonitoring.model;

/**
 * User-facing write-ahead log metrics.
 */
public final class SegmentIndexWalMetrics {

    private final boolean enabled;
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
    private final long appliedLsn;
    private final long syncTotalNanos;
    private final long syncMaxNanos;
    private final long syncBatchBytesTotal;
    private final long syncBatchBytesMax;

    /**
     * Creates WAL metrics.
     *
     * @param enabled whether WAL is enabled
     * @param appendCount append count
     * @param appendBytes appended byte count
     * @param syncCount sync count
     * @param syncFailureCount sync failure count
     * @param corruptionCount corruption count
     * @param truncationCount truncation count
     * @param retainedBytes retained WAL bytes
     * @param segmentCount WAL segment count
     * @param durableLsn durable LSN
     * @param checkpointLsn checkpoint LSN
     * @param pendingSyncBytes pending sync bytes
     * @param appliedLsn applied LSN
     * @param syncTotalNanos total sync nanos
     * @param syncMaxNanos max sync nanos
     * @param syncBatchBytesTotal total sync batch bytes
     * @param syncBatchBytesMax max sync batch bytes
     */
    @SuppressWarnings("java:S107")
    public SegmentIndexWalMetrics(final boolean enabled,
            final long appendCount, final long appendBytes,
            final long syncCount, final long syncFailureCount,
            final long corruptionCount, final long truncationCount,
            final long retainedBytes, final int segmentCount,
            final long durableLsn, final long checkpointLsn,
            final long pendingSyncBytes, final long appliedLsn,
            final long syncTotalNanos, final long syncMaxNanos,
            final long syncBatchBytesTotal, final long syncBatchBytesMax) {
        this.enabled = enabled;
        this.appendCount = MetricModelValidation.nonNegative(appendCount,
                "appendCount");
        this.appendBytes = MetricModelValidation.nonNegative(appendBytes,
                "appendBytes");
        this.syncCount = MetricModelValidation.nonNegative(syncCount,
                "syncCount");
        this.syncFailureCount = MetricModelValidation.nonNegative(
                syncFailureCount, "syncFailureCount");
        this.corruptionCount = MetricModelValidation.nonNegative(
                corruptionCount, "corruptionCount");
        this.truncationCount = MetricModelValidation.nonNegative(
                truncationCount, "truncationCount");
        this.retainedBytes = MetricModelValidation.nonNegative(retainedBytes,
                "retainedBytes");
        this.segmentCount = MetricModelValidation.nonNegative(segmentCount,
                "segmentCount");
        this.durableLsn = MetricModelValidation.nonNegative(durableLsn,
                "durableLsn");
        this.checkpointLsn = MetricModelValidation.nonNegative(checkpointLsn,
                "checkpointLsn");
        this.pendingSyncBytes = MetricModelValidation.nonNegative(
                pendingSyncBytes, "pendingSyncBytes");
        this.appliedLsn = MetricModelValidation.nonNegative(appliedLsn,
                "appliedLsn");
        this.syncTotalNanos = MetricModelValidation.nonNegative(
                syncTotalNanos, "syncTotalNanos");
        this.syncMaxNanos = MetricModelValidation.nonNegative(syncMaxNanos,
                "syncMaxNanos");
        this.syncBatchBytesTotal = MetricModelValidation.nonNegative(
                syncBatchBytesTotal, "syncBatchBytesTotal");
        this.syncBatchBytesMax = MetricModelValidation.nonNegative(
                syncBatchBytesMax, "syncBatchBytesMax");
    }

    /**
     * Returns whether WAL is enabled.
     *
     * @return true when WAL is enabled
     */
    public boolean enabled() {
        return enabled;
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
     * Returns appended byte count.
     *
     * @return appended byte count
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
     * Returns applied LSN.
     *
     * @return applied LSN
     */
    public long appliedLsn() {
        return appliedLsn;
    }

    /**
     * Returns checkpoint lag in LSN units.
     *
     * @return checkpoint lag
     */
    public long checkpointLagLsn() {
        return Math.max(0L, appliedLsn - checkpointLsn);
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

    /**
     * Returns average sync nanos.
     *
     * @return average sync nanos
     */
    public long syncAverageNanos() {
        if (syncCount <= 0L) {
            return 0L;
        }
        return syncTotalNanos / syncCount;
    }

    /**
     * Returns average sync batch bytes.
     *
     * @return average sync batch bytes
     */
    public long syncAverageBatchBytes() {
        if (syncCount <= 0L) {
            return 0L;
        }
        return syncBatchBytesTotal / syncCount;
    }
}
