package org.hestiastore.index.segmentindex.core.infrastructure;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.observability.IndexExecutorMetricsAccess;

/**
 * Observed thread-pool wrapper that exposes executor metrics snapshots.
 */
final class ObservedThreadPool {

    private final ThreadPoolExecutor executor;
    private final int queueCapacity;
    private final LongAdder rejectedTaskCount;
    private final LongAdder callerRunsCount;

    ObservedThreadPool(final ThreadPoolExecutor executor,
            final int queueCapacity, final LongAdder rejectedTaskCount,
            final LongAdder callerRunsCount) {
        this.executor = Vldtn.requireNonNull(executor, "executor");
        this.queueCapacity = Math.max(0, queueCapacity);
        this.rejectedTaskCount = Vldtn.requireNonNull(rejectedTaskCount,
                "rejectedTaskCount");
        this.callerRunsCount = Vldtn.requireNonNull(callerRunsCount,
                "callerRunsCount");
    }

    ExecutorService executor() {
        return executor;
    }

    IndexExecutorMetricsAccess snapshot() {
        return new IndexExecutorMetricsSnapshot(
                executor.getActiveCount(), executor.getQueue().size(),
                queueCapacity, executor.getCompletedTaskCount(),
                rejectedTaskCount.sum(), callerRunsCount.sum());
    }
}
