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

    public static SegmentMaintenanceDecision none() {
        return NONE;
    }

    public static SegmentMaintenanceDecision flushOnly() {
        return new SegmentMaintenanceDecision(true, false);
    }

    public static SegmentMaintenanceDecision compactOnly() {
        return new SegmentMaintenanceDecision(false, true);
    }

    public boolean shouldFlush() {
        return flush;
    }

    public boolean shouldCompact() {
        return compact;
    }
}
