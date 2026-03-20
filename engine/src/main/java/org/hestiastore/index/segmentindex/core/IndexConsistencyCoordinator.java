package org.hestiastore.index.segmentindex.core;

import java.util.function.Predicate;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

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
    private final Runnable scheduleBackgroundSplitScan;
    private final Predicate<SegmentId> startupSegmentFilter;
    private boolean startupSegmentLockValidationEnabled;

    IndexConsistencyCoordinator(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final TypeDescriptor<K> keyTypeDescriptor,
            final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator,
            final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop) {
        this(keyToSegmentMap::checkUniqueSegmentIds,
                segmentFilter -> new IndexConsistencyChecker<>(keyToSegmentMap,
                        segmentRegistry, keyTypeDescriptor, segmentFilter)
                                .checkAndRepairConsistency(),
                recoveryCleanupCoordinator::cleanupOrphanedSegmentDirectories,
                backgroundSplitPolicyLoop::scheduleScan,
                recoveryCleanupCoordinator::hasSegmentLockFile);
    }

    IndexConsistencyCoordinator(final Runnable verifyUniqueSegmentIds,
            final ConsistencyCheckRunner consistencyCheckRunner,
            final Runnable cleanupOrphanedSegmentDirectories,
            final Runnable scheduleBackgroundSplitScan,
            final Predicate<SegmentId> startupSegmentFilter) {
        this.verifyUniqueSegmentIds = Vldtn
                .requireNonNull(verifyUniqueSegmentIds, "verifyUniqueSegmentIds");
        this.consistencyCheckRunner = Vldtn
                .requireNonNull(consistencyCheckRunner, "consistencyCheckRunner");
        this.cleanupOrphanedSegmentDirectories = Vldtn.requireNonNull(
                cleanupOrphanedSegmentDirectories,
                "cleanupOrphanedSegmentDirectories");
        this.scheduleBackgroundSplitScan = Vldtn.requireNonNull(
                scheduleBackgroundSplitScan, "scheduleBackgroundSplitScan");
        this.startupSegmentFilter = Vldtn.requireNonNull(startupSegmentFilter,
                "startupSegmentFilter");
    }

    void checkAndRepairConsistency() {
        verifyUniqueSegmentIds.run();
        consistencyCheckRunner.run(resolveSegmentFilter());
        cleanupOrphanedSegmentDirectories.run();
        scheduleBackgroundSplitScan.run();
    }

    void runStartupConsistencyCheck(final Runnable consistencyCheck) {
        Vldtn.requireNonNull(consistencyCheck, "consistencyCheck");
        startupSegmentLockValidationEnabled = true;
        try {
            consistencyCheck.run();
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
