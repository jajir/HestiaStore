package org.hestiastore.index.segmentindex.core.executor;

import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorMetricsAccess;

/**
 * Immutable executor metrics snapshot owned by infrastructure monitoring.
 */
final class IndexExecutorMetricsSnapshot implements IndexExecutorMetricsAccess {

    private final int activeThreadCount;
    private final int queueSize;
    private final int queueCapacity;
    private final long completedTaskCount;
    private final long rejectedTaskCount;
    private final long callerRunsCount;

    IndexExecutorMetricsSnapshot(final int activeThreadCount,
            final int queueSize, final int queueCapacity,
            final long completedTaskCount, final long rejectedTaskCount,
            final long callerRunsCount) {
        this.activeThreadCount = Math.max(0, activeThreadCount);
        this.queueSize = Math.max(0, queueSize);
        this.queueCapacity = Math.max(0, queueCapacity);
        this.completedTaskCount = Math.max(0L, completedTaskCount);
        this.rejectedTaskCount = Math.max(0L, rejectedTaskCount);
        this.callerRunsCount = Math.max(0L, callerRunsCount);
    }

    @Override
    public int getActiveThreadCount() {
        return activeThreadCount;
    }

    @Override
    public int getQueueSize() {
        return queueSize;
    }

    @Override
    public int getQueueCapacity() {
        return queueCapacity;
    }

    @Override
    public long getCompletedTaskCount() {
        return completedTaskCount;
    }

    @Override
    public long getRejectedTaskCount() {
        return rejectedTaskCount;
    }

    @Override
    public long getCallerRunsCount() {
        return callerRunsCount;
    }
}
