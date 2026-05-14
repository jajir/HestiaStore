package org.hestiastore.index.segmentindex.core.executorregistry;

/**
 * Immutable executor runtime statistics owned by executor infrastructure.
 */
public final class ExecutorStats {

    private final int activeThreadCount;
    private final int queueSize;
    private final int queueCapacity;
    private final long completedTaskCount;
    private final long rejectedTaskCount;
    private final long callerRunsCount;

    ExecutorStats(final int activeThreadCount,
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

    public int getActiveThreadCount() {
        return activeThreadCount;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public long getCompletedTaskCount() {
        return completedTaskCount;
    }

    public long getRejectedTaskCount() {
        return rejectedTaskCount;
    }

    public long getCallerRunsCount() {
        return callerRunsCount;
    }
}
