package org.hestiastore.index.segmentindex.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator;

/**
 * Default {@link SegmentIndexMaintenance} implementation backed by index
 * runtime maintenance commands.
 */
public final class SegmentIndexMaintenanceImpl
        implements SegmentIndexMaintenance {

    private final MaintenanceService maintenanceService;
    private final IndexConsistencyCoordinator<?, ?> consistencyCoordinator;

    public SegmentIndexMaintenanceImpl(
            final MaintenanceService maintenanceService,
            final IndexConsistencyCoordinator<?, ?> consistencyCoordinator) {
        this.maintenanceService = Vldtn.requireNonNull(maintenanceService,
                "maintenanceService");
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
    }

    @Override
    public void compact() {
        maintenanceService.compact();
    }

    @Override
    public void compactAndWait() {
        maintenanceService.compactAndWait();
    }

    @Override
    public void flush() {
        maintenanceService.flush();
    }

    @Override
    public void flushAndWait() {
        maintenanceService.flushAndWait();
    }

    @Override
    public void checkAndRepairConsistency() {
        consistencyCoordinator.checkAndRepairConsistency();
    }
}
