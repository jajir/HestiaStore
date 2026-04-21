package org.hestiastore.index.segmentindex.core.runtime;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.durability.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.BackgroundSplitPolicyAccess;
import org.hestiastore.index.segmentindex.core.maintenance.StableSegmentMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.operation.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.split.BackgroundSplitCoordinator;

/**
 * Holds split-related runtime collaborators that coordinate direct and
 * background segment access.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeSplits<K, V> {

    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator;
    private final BackgroundSplitPolicyAccess<K, V> backgroundSplitPolicyLoop;
    private final DirectSegmentAccess<K, V> directSegmentCoordinator;
    private final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator;

    SegmentIndexRuntimeSplits(
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator,
            final BackgroundSplitPolicyAccess<K, V> backgroundSplitPolicyLoop,
            final DirectSegmentAccess<K, V> directSegmentCoordinator,
            final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator) {
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.stableSegmentCoordinator = Vldtn.requireNonNull(
                stableSegmentCoordinator, "stableSegmentCoordinator");
        this.backgroundSplitPolicyLoop = Vldtn.requireNonNull(
                backgroundSplitPolicyLoop, "backgroundSplitPolicyLoop");
        this.directSegmentCoordinator = Vldtn.requireNonNull(
                directSegmentCoordinator, "directSegmentCoordinator");
        this.recoveryCleanupCoordinator = Vldtn.requireNonNull(
                recoveryCleanupCoordinator, "recoveryCleanupCoordinator");
    }

    BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator() {
        return backgroundSplitCoordinator;
    }

    StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator() {
        return stableSegmentCoordinator;
    }

    BackgroundSplitPolicyAccess<K, V> backgroundSplitPolicyLoop() {
        return backgroundSplitPolicyLoop;
    }

    DirectSegmentAccess<K, V> directSegmentCoordinator() {
        return directSegmentCoordinator;
    }

    IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator() {
        return recoveryCleanupCoordinator;
    }
}
