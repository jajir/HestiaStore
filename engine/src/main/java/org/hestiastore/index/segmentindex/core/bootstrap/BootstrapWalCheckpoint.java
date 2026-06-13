package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceCheckpoint;
import org.hestiastore.index.segmentindex.core.storage.StorageService;

/**
 * Bridges blocking maintenance checkpoints to WAL coordination after storage has
 * finished WAL initialization.
 */
final class BootstrapWalCheckpoint implements MaintenanceCheckpoint {

    private StorageService<?, ?> storageService;

    /**
     * Binds the checkpoint to initialized WAL storage coordination.
     *
     * @param storageService initialized storage service
     */
    void bindStorageService(final StorageService<?, ?> storageService) {
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
    }

    @Override
    public void checkpoint() {
        if (storageService == null) {
            throw new IllegalStateException(
                    "storageService was not initialized.");
        }
        storageService.checkpointWal();
    }
}
