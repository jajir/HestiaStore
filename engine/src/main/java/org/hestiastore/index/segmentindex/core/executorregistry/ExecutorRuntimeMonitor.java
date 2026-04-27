package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorRuntimeAccess;

/**
 * Produces runtime snapshots from observed thread pools without owning the
 * executors themselves.
 */
final class ExecutorRuntimeMonitor {

    private final ObservedThreadPool indexMaintenanceThreadPool;
    private final ObservedThreadPool splitMaintenanceThreadPool;
    private final ObservedThreadPool stableSegmentMaintenanceThreadPool;

    ExecutorRuntimeMonitor(
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
        return new ExecutorRuntimeSnapshot(
                indexMaintenanceThreadPool.snapshot(),
                splitMaintenanceThreadPool.snapshot(),
                stableSegmentMaintenanceThreadPool.snapshot());
    }
}
