package org.hestiastore.index.segmentindex.core.infrastructure;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.observability.IndexExecutorRuntimeAccess;

/**
 * Produces runtime snapshots from observed thread pools without owning the
 * executors themselves.
 */
final class IndexExecutorRuntimeMonitor {

    private final ObservedThreadPool indexMaintenanceThreadPool;
    private final ObservedThreadPool splitMaintenanceThreadPool;
    private final ObservedThreadPool stableSegmentMaintenanceThreadPool;

    IndexExecutorRuntimeMonitor(
            final ObservedThreadPool indexMaintenanceThreadPool,
            final ObservedThreadPool splitMaintenanceThreadPool,
            final ObservedThreadPool stableSegmentMaintenanceThreadPool) {
        this.indexMaintenanceThreadPool = Vldtn.requireNonNull(
                indexMaintenanceThreadPool, "indexMaintenanceThreadPool");
        this.splitMaintenanceThreadPool = Vldtn.requireNonNull(
                splitMaintenanceThreadPool, "splitMaintenanceThreadPool");
        this.stableSegmentMaintenanceThreadPool = Vldtn.requireNonNull(
                stableSegmentMaintenanceThreadPool,
                "stableSegmentMaintenanceThreadPool");
    }

    IndexExecutorRuntimeAccess runtimeSnapshot() {
        return new IndexExecutorRuntimeSnapshot(
                indexMaintenanceThreadPool.snapshot(),
                splitMaintenanceThreadPool.snapshot(),
                stableSegmentMaintenanceThreadPool.snapshot());
    }
}
