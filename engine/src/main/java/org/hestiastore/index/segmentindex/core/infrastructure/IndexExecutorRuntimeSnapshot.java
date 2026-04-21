package org.hestiastore.index.segmentindex.core.infrastructure;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.observability.IndexExecutorMetricsAccess;
import org.hestiastore.index.segmentindex.core.observability.IndexExecutorRuntimeAccess;

/**
 * Immutable executor runtime snapshot owned by infrastructure monitoring.
 */
final class IndexExecutorRuntimeSnapshot implements IndexExecutorRuntimeAccess {

    private final IndexExecutorMetricsAccess indexMaintenance;
    private final IndexExecutorMetricsAccess splitMaintenance;
    private final IndexExecutorMetricsAccess stableSegmentMaintenance;

    IndexExecutorRuntimeSnapshot(
            final IndexExecutorMetricsAccess indexMaintenance,
            final IndexExecutorMetricsAccess splitMaintenance,
            final IndexExecutorMetricsAccess stableSegmentMaintenance) {
        this.indexMaintenance = Vldtn.requireNonNull(indexMaintenance,
                "indexMaintenance");
        this.splitMaintenance = Vldtn.requireNonNull(splitMaintenance,
                "splitMaintenance");
        this.stableSegmentMaintenance = Vldtn.requireNonNull(
                stableSegmentMaintenance, "stableSegmentMaintenance");
    }

    @Override
    public IndexExecutorMetricsAccess getIndexMaintenance() {
        return indexMaintenance;
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
