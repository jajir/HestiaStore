package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.IndexException;
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
        failure = shutdownAndAwait("indexMaintenance",
                indexMaintenanceExecutor, failure);
        failure = shutdownAndAwait("splitMaintenance",
                splitMaintenanceExecutor, failure);
        failure = shutdownAndAwait("splitPolicy",
                splitPolicyScheduler, failure);
        failure = shutdownAndAwait("segmentMaintenance",
                stableSegmentMaintenanceExecutor, failure);
        failure = shutdownAndAwait("registryMaintenance",
                registryMaintenanceExecutor, failure);
        return failure;
    }

    private RuntimeException shutdownAndAwait(final String executorName,
            final ExecutorService executor,
            final RuntimeException failure) {
        RuntimeException nextFailure = failure;
        executor.shutdown();
        boolean interrupted = false;
        try {
            if (!executor.awaitTermination(shutdownTimeoutMillis,
                    TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                nextFailure = appendFailure(nextFailure, new IndexException(
                        String.format(
                                "Executor '%s' did not terminate within %d ms.",
                                executorName, shutdownTimeoutMillis)));
            }
        } catch (final InterruptedException e) {
            interrupted = true;
            executor.shutdownNow();
            nextFailure = appendFailure(nextFailure, new IndexException(
                    String.format("Executor '%s' shutdown was interrupted.",
                            executorName),
                    e));
        } catch (final RuntimeException e) {
            nextFailure = appendFailure(nextFailure, e);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return nextFailure;
    }

    private RuntimeException appendFailure(final RuntimeException failure,
            final RuntimeException nextFailure) {
        if (failure == null) {
            return nextFailure;
        }
        failure.addSuppressed(nextFailure);
        return failure;
    }
}
