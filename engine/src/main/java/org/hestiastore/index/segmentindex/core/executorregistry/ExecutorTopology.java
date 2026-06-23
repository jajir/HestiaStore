package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.Vldtn;

/**
 * Holds executor instances together with their close ordering.
 */
final class ExecutorTopology {

    private final ExecutorService indexMaintenanceExecutor;
    private final ExecutorService splitMaintenanceExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final ExecutorService stableSegmentMaintenanceExecutor;
    private final ExecutorService registryMaintenanceExecutor;
    private final int shutdownTimeoutMillis;

    ExecutorTopology(final ExecutorService indexMaintenanceExecutor,
            final ExecutorService splitMaintenanceExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final ExecutorService stableSegmentMaintenanceExecutor,
            final ExecutorService registryMaintenanceExecutor,
            final int shutdownTimeoutMillis) {
        this.indexMaintenanceExecutor = Vldtn.requireNonNull(
                indexMaintenanceExecutor, "indexMaintenanceExecutor");
        this.splitMaintenanceExecutor = Vldtn.requireNonNull(
                splitMaintenanceExecutor, "splitMaintenanceExecutor");
        this.splitPolicyScheduler = Vldtn.requireNonNull(splitPolicyScheduler,
                "splitPolicyScheduler");
        this.stableSegmentMaintenanceExecutor = Vldtn.requireNonNull(
                stableSegmentMaintenanceExecutor,
                "stableSegmentMaintenanceExecutor");
        this.registryMaintenanceExecutor = Vldtn.requireNonNull(
                registryMaintenanceExecutor, "registryMaintenanceExecutor");
        this.shutdownTimeoutMillis = Vldtn.requireGreaterThanZero(
                shutdownTimeoutMillis, "shutdownTimeoutMillis");
    }

    ExecutorService indexMaintenanceExecutor() {
        return indexMaintenanceExecutor;
    }

    ExecutorService splitMaintenanceExecutor() {
        return splitMaintenanceExecutor;
    }

    ScheduledExecutorService splitPolicyScheduler() {
        return splitPolicyScheduler;
    }

    ExecutorService stableSegmentMaintenanceExecutor() {
        return stableSegmentMaintenanceExecutor;
    }

    ExecutorService registryMaintenanceExecutor() {
        return registryMaintenanceExecutor;
    }

    RuntimeException shutdownExecutorsInCloseOrder() {
        RuntimeException failure = null;
        failure = ExecutorShutdown.shutdownAndAwait("indexMaintenance",
                indexMaintenanceExecutor, shutdownTimeoutMillis, failure);
        failure = ExecutorShutdown.shutdownAndAwait("splitPolicy",
                splitPolicyScheduler, shutdownTimeoutMillis, failure);
        failure = ExecutorShutdown.shutdownAndAwait("registryMaintenance",
                registryMaintenanceExecutor, shutdownTimeoutMillis, failure);
        return failure;
    }
}
