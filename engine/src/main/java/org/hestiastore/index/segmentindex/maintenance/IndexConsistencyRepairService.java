package org.hestiastore.index.segmentindex.maintenance;

/**
 * Repairs index consistency and schedules any runtime follow-up work required
 * after the repair.
 */
public interface IndexConsistencyRepairService {

    /**
     * Checks index consistency, repairs correctable inconsistencies, and
     * schedules required runtime follow-up work.
     */
    void checkAndRepairConsistency();
}
