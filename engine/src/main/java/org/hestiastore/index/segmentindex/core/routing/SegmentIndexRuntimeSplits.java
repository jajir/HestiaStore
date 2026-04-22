package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.StableSegmentMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.splitplanner.SplitPlanner;

/**
 * Holds split-related runtime collaborators that coordinate direct and
 * background segment access.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntimeSplits<K, V> {

    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator;
    private final SplitPlanner<K, V> splitPlanner;
    private final DirectSegmentAccess<K, V> directSegmentCoordinator;
    private final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator;

    public SegmentIndexRuntimeSplits(
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator,
            final SplitPlanner<K, V> splitPlanner,
            final DirectSegmentAccess<K, V> directSegmentCoordinator,
            final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator) {
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.stableSegmentCoordinator = Vldtn.requireNonNull(
                stableSegmentCoordinator, "stableSegmentCoordinator");
        this.splitPlanner = Vldtn.requireNonNull(splitPlanner,
                "splitPlanner");
        this.directSegmentCoordinator = Vldtn.requireNonNull(
                directSegmentCoordinator, "directSegmentCoordinator");
        this.recoveryCleanupCoordinator = Vldtn.requireNonNull(
                recoveryCleanupCoordinator, "recoveryCleanupCoordinator");
    }

    public BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator() {
        return backgroundSplitCoordinator;
    }

    public StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator() {
        return stableSegmentCoordinator;
    }

    public SplitPlanner<K, V> splitPlanner() {
        return splitPlanner;
    }

    public DirectSegmentAccess<K, V> directSegmentCoordinator() {
        return directSegmentCoordinator;
    }

    public IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator() {
        return recoveryCleanupCoordinator;
    }
}
