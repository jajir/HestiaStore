package org.hestiastore.index.segmentasync;

import org.hestiastore.index.segment.SegmentResult;

/**
 * Optional interface for segments that can run maintenance synchronously.
 */
public interface SegmentMaintenanceBlocking {

    /**
     * Runs a flush immediately on the current thread.
     *
     * @return flush result
     */
    SegmentResult<Void> flushBlocking();

    /**
     * Runs a compaction immediately on the current thread.
     *
     * @return compact result
     */
    SegmentResult<Void> compactBlocking();
}
