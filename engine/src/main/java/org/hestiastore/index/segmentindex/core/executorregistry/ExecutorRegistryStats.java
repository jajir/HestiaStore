package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.Vldtn;

/**
 * Immutable executor registry runtime statistics.
 */
public final class ExecutorRegistryStats {

    private final ExecutorStats indexMaintenance;
    private final ExecutorStats splitMaintenance;
    private final ExecutorStats stableSegmentMaintenance;

    ExecutorRegistryStats(
            final ExecutorStats indexMaintenance,
            final ExecutorStats splitMaintenance,
            final ExecutorStats stableSegmentMaintenance) {
        this.indexMaintenance = Vldtn.requireNonNull(indexMaintenance,
                "indexMaintenance");
        this.splitMaintenance = Vldtn.requireNonNull(splitMaintenance,
                "splitMaintenance");
        this.stableSegmentMaintenance = Vldtn.requireNonNull(
                stableSegmentMaintenance, "stableSegmentMaintenance");
    }

    public ExecutorStats getIndexMaintenance() {
        return indexMaintenance;
    }

    public ExecutorStats getSplitMaintenance() {
        return splitMaintenance;
    }

    public ExecutorStats getStableSegmentMaintenance() {
        return stableSegmentMaintenance;
    }
}
