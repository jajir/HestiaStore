package org.hestiastore.index.segmentindex.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;

/**
 * Default {@link SegmentIndexMaintenance} implementation backed by index
 * runtime maintenance commands.
 */
public final class SegmentIndexMaintenanceImpl
        implements SegmentIndexMaintenance {

    private final MaintenanceService maintenanceService;
    private final IndexConsistencyRepairService consistencyRepairService;

    /**
     * Creates a segment-index maintenance implementation.
     *
     * @param maintenanceService segment maintenance command service
     * @param consistencyRepairService storage repair and runtime follow-up
     *            service
     */
    public SegmentIndexMaintenanceImpl(
            final MaintenanceService maintenanceService,
            final IndexConsistencyRepairService consistencyRepairService) {
        this.maintenanceService = Vldtn.requireNonNull(maintenanceService,
                "maintenanceService");
        this.consistencyRepairService = Vldtn.requireNonNull(
                consistencyRepairService, "consistencyRepairService");
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
        consistencyRepairService.checkAndRepairConsistency();
    }
}
