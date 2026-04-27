package org.hestiastore.index.segmentindex.core.split;

/**
 * Immutable snapshot of split counters exposed to metrics collection.
 */
public final class SplitMetricsSnapshot {

    private final int splitInFlightCount;
    private final int splitBlockedCount;

    /**
     * Creates a split metrics snapshot.
     *
     * @param splitInFlightCount number of scheduled or running split tasks
     * @param splitBlockedCount number of blocked segments with active split
     *        work
     */
    public SplitMetricsSnapshot(final int splitInFlightCount,
            final int splitBlockedCount) {
        this.splitInFlightCount = splitInFlightCount;
        this.splitBlockedCount = splitBlockedCount;
    }

    /**
     * @return number of scheduled or running split tasks
     */
    public int splitInFlightCount() {
        return splitInFlightCount;
    }

    /**
     * @return number of blocked segments with active split work
     */
    public int splitBlockedCount() {
        return splitBlockedCount;
    }
}
