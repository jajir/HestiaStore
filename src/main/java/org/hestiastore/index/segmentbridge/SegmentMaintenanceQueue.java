package org.hestiastore.index.segmentbridge;

import java.util.concurrent.CompletionStage;

import org.hestiastore.index.segment.SegmentResult;

/**
 * Optional interface for segments that can schedule custom maintenance tasks.
 */
public interface SegmentMaintenanceQueue {

    /**
     * Enqueues a maintenance task and returns a completion stage.
     *
     * @param taskType maintenance task type
     * @param task     task to execute
     * @return completion stage for the task
     */
    CompletionStage<SegmentResult<Void>> submitMaintenanceTask(
            SegmentMaintenanceTask taskType, Runnable task);
}
