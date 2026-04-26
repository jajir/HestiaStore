package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Vldtn;

/**
 * Holds executor instances together with their close ordering.
 */
final class ExecutorTopology {

    private final ExecutorService indexMaintenanceExecutor;
    private final ExecutorService splitMaintenanceExecutor;
    private final LazyExecutorReference<ScheduledExecutorService> splitPolicyScheduler;
    private final ExecutorService stableSegmentMaintenanceExecutor;
    private final LazyExecutorReference<ExecutorService> registryMaintenanceExecutor;

    ExecutorTopology(final ExecutorService indexMaintenanceExecutor,
            final ExecutorService splitMaintenanceExecutor,
            final LazyExecutorReference<ScheduledExecutorService> splitPolicyScheduler,
            final ExecutorService stableSegmentMaintenanceExecutor,
            final LazyExecutorReference<ExecutorService> registryMaintenanceExecutor) {
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
    }

    ExecutorService indexMaintenanceExecutor() {
        return indexMaintenanceExecutor;
    }

    ExecutorService splitMaintenanceExecutor() {
        return splitMaintenanceExecutor;
    }

    ScheduledExecutorService splitPolicyScheduler() {
        return splitPolicyScheduler.get();
    }

    ExecutorService stableSegmentMaintenanceExecutor() {
        return stableSegmentMaintenanceExecutor;
    }

    ExecutorService registryMaintenanceExecutor() {
        return registryMaintenanceExecutor.get();
    }

    RuntimeException shutdownExecutorsInCloseOrder() {
        RuntimeException failure = null;
        failure = shutdownAndAwait(indexMaintenanceExecutor, failure);
        failure = shutdownAndAwait(splitMaintenanceExecutor, failure);
        failure = shutdownAndAwait(splitPolicyScheduler.getIfCreated(), failure);
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
