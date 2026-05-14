package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.Vldtn;

/**
 * Produces stats snapshots from observed thread pools without owning the
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

    ExecutorRegistryStats statsSnapshot() {
        return new ExecutorRegistryStats(
                indexMaintenanceThreadPool.statsSnapshot(),
                splitMaintenanceThreadPool.statsSnapshot(),
                stableSegmentMaintenanceThreadPool.statsSnapshot());
    }
}
