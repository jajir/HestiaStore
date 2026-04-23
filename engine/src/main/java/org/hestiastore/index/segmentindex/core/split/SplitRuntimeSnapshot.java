package org.hestiastore.index.segmentindex.core.split;

/**
 * Immutable runtime snapshot for split-service state.
 */
public final class SplitRuntimeSnapshot {

    private final int splitInFlightCount;
    private final int splitBlockedCount;

    /**
     * Creates a split runtime snapshot.
     *
     * @param splitInFlightCount number of scheduled or running split tasks
     * @param splitBlockedCount number of blocked segments with active split
     *        work
     */
    public SplitRuntimeSnapshot(final int splitInFlightCount,
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
