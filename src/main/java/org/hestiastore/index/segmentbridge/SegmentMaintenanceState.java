package org.hestiastore.index.segmentbridge;

import org.hestiastore.index.Vldtn;

/**
 * Tracks which maintenance task is currently active for a segment.
 */
final class SegmentMaintenanceState {

    private SegmentMaintenanceTask activeTask;

    void runExclusive(final SegmentMaintenanceTask task,
            final Runnable action) {
        Vldtn.requireNonNull(task, "task");
        Vldtn.requireNonNull(action, "action");
        start(task);
        try {
            action.run();
        } finally {
            finish(task);
        }
    }

    private synchronized void start(final SegmentMaintenanceTask task) {
        activeTask = task;
    }

    private synchronized void finish(final SegmentMaintenanceTask task) {
        if (activeTask == task) {
            activeTask = null;
        }
    }

    synchronized SegmentMaintenanceTask getActiveTask() {
        return activeTask;
    }
}
