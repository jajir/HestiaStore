package org.hestiastore.index.segment;

/**
 * Describes which maintenance operations should be scheduled.
 */
public final class SegmentMaintenanceDecision {

    private static final SegmentMaintenanceDecision NONE = new SegmentMaintenanceDecision(
            false, false);

    private final boolean flush;
    private final boolean compact;

    private SegmentMaintenanceDecision(final boolean flush,
            final boolean compact) {
        this.flush = flush;
        this.compact = compact;
    }

    /**
     * Returns a decision that performs no maintenance.
     *
     * @return decision with no maintenance actions
     */
    public static SegmentMaintenanceDecision none() {
        return NONE;
    }

    /**
     * Returns a decision that requests a flush only.
     *
     * @return decision that flushes the write cache
     */
    public static SegmentMaintenanceDecision flushOnly() {
        return new SegmentMaintenanceDecision(true, false);
    }

    /**
     * Returns a decision that requests a compaction only.
     *
     * @return decision that compacts the segment
     */
    public static SegmentMaintenanceDecision compactOnly() {
        return new SegmentMaintenanceDecision(false, true);
    }

    /**
     * Returns true when a flush should be scheduled.
     *
     * @return true when flush is requested
     */
    public boolean shouldFlush() {
        return flush;
    }

    /**
     * Returns true when a compaction should be scheduled.
     *
     * @return true when compaction is requested
     */
    public boolean shouldCompact() {
        return compact;
    }
}
