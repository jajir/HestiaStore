package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorMetricsAccess;
import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorRuntimeAccess;

/**
 * Immutable executor runtime snapshot owned by infrastructure monitoring.
 */
final class IndexExecutorRuntimeSnapshot implements IndexExecutorRuntimeAccess {

    private final IndexExecutorMetricsAccess splitPlanner;
    private final IndexExecutorMetricsAccess splitMaintenance;
    private final IndexExecutorMetricsAccess stableSegmentMaintenance;

    IndexExecutorRuntimeSnapshot(
            final IndexExecutorMetricsAccess splitPlanner,
            final IndexExecutorMetricsAccess splitMaintenance,
            final IndexExecutorMetricsAccess stableSegmentMaintenance) {
        this.splitPlanner = Vldtn.requireNonNull(splitPlanner,
                "splitPlanner");
        this.splitMaintenance = Vldtn.requireNonNull(splitMaintenance,
                "splitMaintenance");
        this.stableSegmentMaintenance = Vldtn.requireNonNull(
                stableSegmentMaintenance, "stableSegmentMaintenance");
    }

    @Override
    public IndexExecutorMetricsAccess getSplitPlanner() {
        return splitPlanner;
    }

    @Override
    public IndexExecutorMetricsAccess getSplitMaintenance() {
        return splitMaintenance;
    }

    @Override
    public IndexExecutorMetricsAccess getStableSegmentMaintenance() {
        return stableSegmentMaintenance;
    }
}
