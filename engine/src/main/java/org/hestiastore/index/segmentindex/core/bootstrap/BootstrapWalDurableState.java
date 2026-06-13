package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.storage.WalCheckpointDurableState;

/**
 * Flushes maintenance-owned durable state before WAL checkpoint retention
 * advances.
 */
final class BootstrapWalDurableState implements WalCheckpointDurableState {

    private final MaintenanceService maintenance;

    /**
     * Creates WAL durable state backed by blocking maintenance flush.
     *
     * @param maintenance maintenance service that flushes mapped index state
     */
    BootstrapWalDurableState(final MaintenanceService maintenance) {
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
    }

    @Override
    public void flushBeforeWalCheckpoint() {
        maintenance.flushAndWait();
    }
}
