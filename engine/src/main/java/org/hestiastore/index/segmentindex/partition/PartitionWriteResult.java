package org.hestiastore.index.segmentindex.partition;

/**
 * Result of a buffered write into the partition runtime.
 *
 * @author honza
 */
public final class PartitionWriteResult {

    private static final PartitionWriteResult BUSY = new PartitionWriteResult(
            PartitionWriteResultStatus.BUSY, false);
    private static final PartitionWriteResult OK_NO_DRAIN = new PartitionWriteResult(
            PartitionWriteResultStatus.OK, false);
    private static final PartitionWriteResult OK_WITH_DRAIN = new PartitionWriteResult(
            PartitionWriteResultStatus.OK, true);

    private final PartitionWriteResultStatus status;
    private final boolean drainRecommended;

    private PartitionWriteResult(final PartitionWriteResultStatus status,
            final boolean drainRecommended) {
        this.status = status;
        this.drainRecommended = drainRecommended;
    }

    public static PartitionWriteResult busy() {
        return BUSY;
    }

    public static PartitionWriteResult ok(final boolean drainRecommended) {
        return drainRecommended ? OK_WITH_DRAIN : OK_NO_DRAIN;
    }

    public PartitionWriteResultStatus getStatus() {
        return status;
    }

    public boolean isDrainRecommended() {
        return drainRecommended;
    }
}
