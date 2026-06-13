package org.hestiastore.index.segmentindex.core.maintenance;

/**
 * Checkpoints durable index state after blocking maintenance has flushed mapped
 * segment and routing changes.
 */
@FunctionalInterface
public interface MaintenanceCheckpoint {

    /**
     * Runs the durable-state checkpoint required after settled maintenance.
     */
    void checkpoint();
}
