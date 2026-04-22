package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorRuntimeAccess;

/**
 * Produces runtime snapshots from observed thread pools without owning the
 * executors themselves.
 */
final class IndexExecutorRuntimeMonitor {

    private final ObservedThreadPool splitPlannerThreadPool;
    private final ObservedThreadPool splitMaintenanceThreadPool;
    private final ObservedThreadPool stableSegmentMaintenanceThreadPool;

    IndexExecutorRuntimeMonitor(
            final ObservedThreadPool splitPlannerThreadPool,
            final ObservedThreadPool splitMaintenanceThreadPool,
            final ObservedThreadPool stableSegmentMaintenanceThreadPool) {
        this.splitPlannerThreadPool = Vldtn.requireNonNull(
                splitPlannerThreadPool, "splitPlannerThreadPool");
        this.splitMaintenanceThreadPool = Vldtn.requireNonNull(
                splitMaintenanceThreadPool, "splitMaintenanceThreadPool");
        this.stableSegmentMaintenanceThreadPool = Vldtn.requireNonNull(
                stableSegmentMaintenanceThreadPool,
                "stableSegmentMaintenanceThreadPool");
    }

    IndexExecutorRuntimeAccess runtimeSnapshot() {
        return new IndexExecutorRuntimeSnapshot(
                splitPlannerThreadPool.snapshot(),
                splitMaintenanceThreadPool.snapshot(),
                stableSegmentMaintenanceThreadPool.snapshot());
    }
}
