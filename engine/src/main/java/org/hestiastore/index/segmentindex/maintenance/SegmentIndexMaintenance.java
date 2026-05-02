package org.hestiastore.index.segmentindex.maintenance;

/**
 * Operational maintenance API for one segment index.
 */
public interface SegmentIndexMaintenance {

    /**
     * Starts a compaction pass over in-memory and on-disk data structures. The
     * call returns after compaction is accepted by each segment.
     */
    void compact();

    /**
     * Starts a compaction pass and waits until all segment maintenance
     * operations complete. Do not call from a segment maintenance executor
     * thread.
     */
    void compactAndWait();

    /**
     * Starts flushing in-memory data to disk. The call returns after flush is
     * accepted by each segment.
     */
    void flush();

    /**
     * Starts flushing in-memory data to disk and waits until all segment
     * maintenance operations complete. Do not call from a segment maintenance
     * executor thread.
     */
    void flushAndWait();

    /**
     * Checks the internal consistency of all index segments and associated data
     * descriptions. Correctable inconsistencies may be repaired automatically.
     */
    void checkAndRepairConsistency();
}
