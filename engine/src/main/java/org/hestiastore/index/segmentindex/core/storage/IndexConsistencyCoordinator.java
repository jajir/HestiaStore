package org.hestiastore.index.segmentindex.core.storage;

import java.util.function.Predicate;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Owns explicit consistency checks and the startup-only lock validation mode.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexConsistencyCoordinator<K, V> {

    @FunctionalInterface
    interface ConsistencyCheckRunner {

        void run(Predicate<SegmentId> segmentFilter);
    }

    private final Runnable verifyUniqueSegmentIds;
    private final ConsistencyCheckRunner consistencyCheckRunner;
    private final Runnable cleanupOrphanedSegmentDirectories;
    private final Predicate<SegmentId> startupSegmentFilter;
    private boolean startupSegmentLockValidationEnabled;

    IndexConsistencyCoordinator(final Runnable verifyUniqueSegmentIds,
            final ConsistencyCheckRunner consistencyCheckRunner,
            final Runnable cleanupOrphanedSegmentDirectories,
            final Predicate<SegmentId> startupSegmentFilter) {
        this.verifyUniqueSegmentIds = Vldtn
                .requireNonNull(verifyUniqueSegmentIds, "verifyUniqueSegmentIds");
        this.consistencyCheckRunner = Vldtn
                .requireNonNull(consistencyCheckRunner, "consistencyCheckRunner");
        this.cleanupOrphanedSegmentDirectories = Vldtn.requireNonNull(
                cleanupOrphanedSegmentDirectories,
                "cleanupOrphanedSegmentDirectories");
        this.startupSegmentFilter = Vldtn.requireNonNull(startupSegmentFilter,
                "startupSegmentFilter");
    }

    void checkAndRepairConsistency() {
        verifyUniqueSegmentIds.run();
        consistencyCheckRunner.run(resolveSegmentFilter());
        cleanupOrphanedSegmentDirectories.run();
    }

    void runStartupConsistencyCheck() {
        startupSegmentLockValidationEnabled = true;
        try {
            checkAndRepairConsistency();
        } finally {
            startupSegmentLockValidationEnabled = false;
        }
    }

    private Predicate<SegmentId> resolveSegmentFilter() {
        if (startupSegmentLockValidationEnabled) {
            return startupSegmentFilter;
        }
        return segmentId -> true;
    }
}
