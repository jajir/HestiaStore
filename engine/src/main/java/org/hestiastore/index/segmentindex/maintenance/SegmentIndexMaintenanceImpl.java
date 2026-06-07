package org.hestiastore.index.segmentindex.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.storage.StorageService;

/**
 * Default {@link SegmentIndexMaintenance} implementation backed by index
 * runtime maintenance commands.
 */
public final class SegmentIndexMaintenanceImpl
        implements SegmentIndexMaintenance {

    private final MaintenanceService maintenanceService;
    private final StorageService<?, ?> storageService;
    private final Runnable requestFullSplitScan;

    public SegmentIndexMaintenanceImpl(
            final MaintenanceService maintenanceService,
            final StorageService<?, ?> storageService,
            final Runnable requestFullSplitScan) {
        this.maintenanceService = Vldtn.requireNonNull(maintenanceService,
                "maintenanceService");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.requestFullSplitScan = Vldtn.requireNonNull(
                requestFullSplitScan, "requestFullSplitScan");
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
        storageService.checkAndRepairConsistency();
        requestFullSplitScan.run();
    }
}
