package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Vldtn;

/**
 * Holds executor instances together with their close ordering.
 */
final class IndexExecutorTopology {

    private final ExecutorService splitPlannerExecutor;
    private final ExecutorService splitMaintenanceExecutor;
    private final ExecutorService stableSegmentMaintenanceExecutor;
    private final LazyExecutorReference<ExecutorService> registryMaintenanceExecutor;

    IndexExecutorTopology(final ExecutorService splitPlannerExecutor,
            final ExecutorService splitMaintenanceExecutor,
            final ExecutorService stableSegmentMaintenanceExecutor,
            final LazyExecutorReference<ExecutorService> registryMaintenanceExecutor) {
        this.splitPlannerExecutor = Vldtn.requireNonNull(
                splitPlannerExecutor, "splitPlannerExecutor");
        this.splitMaintenanceExecutor = Vldtn.requireNonNull(
                splitMaintenanceExecutor, "splitMaintenanceExecutor");
        this.stableSegmentMaintenanceExecutor = Vldtn.requireNonNull(
                stableSegmentMaintenanceExecutor,
                "stableSegmentMaintenanceExecutor");
        this.registryMaintenanceExecutor = Vldtn.requireNonNull(
                registryMaintenanceExecutor, "registryMaintenanceExecutor");
    }

    ExecutorService splitPlannerExecutor() {
        return splitPlannerExecutor;
    }

    ExecutorService stableSegmentMaintenanceExecutor() {
        return stableSegmentMaintenanceExecutor;
    }

    ExecutorService splitMaintenanceExecutor() {
        return splitMaintenanceExecutor;
    }

    ExecutorService registryMaintenanceExecutor() {
        return registryMaintenanceExecutor.get();
    }

    RuntimeException shutdownExecutorsInCloseOrder() {
        RuntimeException failure = null;
        failure = shutdownAndAwait(splitPlannerExecutor, failure);
        failure = shutdownAndAwait(splitMaintenanceExecutor, failure);
        failure = shutdownAndAwait(stableSegmentMaintenanceExecutor, failure);
        failure = shutdownAndAwait(registryMaintenanceExecutor.getIfCreated(),
                failure);
        return failure;
    }

    private RuntimeException shutdownAndAwait(final ExecutorService executor,
            final RuntimeException failure) {
        if (executor == null) {
            return failure;
        }
        RuntimeException nextFailure = failure;
        executor.shutdown();
        boolean interrupted = false;
        try {
            while (!executor.isTerminated()) {
                executor.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (final InterruptedException e) {
            interrupted = true;
            executor.shutdownNow();
        } catch (final RuntimeException e) {
            if (nextFailure == null) {
                nextFailure = e;
            } else {
                nextFailure.addSuppressed(e);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return nextFailure;
    }
}
