package org.hestiastore.index.segmentindex.core.facade;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.consistency.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;

/**
 * Owns tracked maintenance commands executed against the index runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexMaintenanceCommands<K, V> {

    private final SegmentIndexTrackedOperationRunner<K, V> trackedRunner;
    private final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess;
    private final IndexConsistencyCoordinator<K, V> consistencyCoordinator;

    public SegmentIndexMaintenanceCommands(
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        this.trackedRunner = Vldtn.requireNonNull(trackedRunner,
                "trackedRunner");
        this.maintenanceAccess = Vldtn.requireNonNull(maintenanceAccess,
                "maintenanceAccess");
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
    }

    public void checkAndRepairConsistency() {
        trackedRunner
                .runTrackedVoid(consistencyCoordinator::checkAndRepairConsistency);
    }

    public void compact() {
        trackedRunner.runTrackedVoid(maintenanceAccess::compact);
    }

    public void compactAndWait() {
        trackedRunner.runTrackedVoid(maintenanceAccess::compactAndWait);
    }

    public void flush() {
        trackedRunner.runTrackedVoid(maintenanceAccess::flush);
    }

    public void flushAndWait() {
        trackedRunner.runTrackedVoid(maintenanceAccess::flushAndWait);
    }
}
