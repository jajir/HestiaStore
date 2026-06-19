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

    private final MaintenanceService<?, ?> maintenanceService;
    private final StorageService<?, ?> storageService;

    /**
     * Creates a segment-index maintenance implementation.
     *
     * @param maintenanceService segment maintenance command service
     * @param storageService storage service used for consistency repair
     */
    public SegmentIndexMaintenanceImpl(
            final MaintenanceService<?, ?> maintenanceService,
            final StorageService<?, ?> storageService) {
        this.maintenanceService = Vldtn.requireNonNull(maintenanceService,
                "maintenanceService");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
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
    }
}
