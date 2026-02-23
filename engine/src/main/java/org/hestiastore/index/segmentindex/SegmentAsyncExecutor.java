package org.hestiastore.index.segmentindex;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Segment-maintenance executor facade backed by an externally owned executor.
 */
public final class SegmentAsyncExecutor extends AbstractCloseableResource {

    private final ExecutorService executor;
    private final ThreadPoolExecutor threadPoolExecutor;

    /**
     * Creates segment-maintenance executor facade.
     *
     * @param executor shared executor service
     */
    public SegmentAsyncExecutor(final ExecutorService executor) {
        this.executor = Vldtn.requireNonNull(executor, "executor");
        this.threadPoolExecutor = executor instanceof ThreadPoolExecutor tp
                ? tp
                : null;
    }

    /**
     * Returns the underlying executor service.
     *
     * @return executor service
     */
    public ExecutorService getExecutor() {
        return executor;
    }

    int getQueueSize() {
        if (threadPoolExecutor == null) {
            return 0;
        }
        return threadPoolExecutor.getQueue().size();
    }

    int getActiveCount() {
        if (threadPoolExecutor == null) {
            return 0;
        }
        return threadPoolExecutor.getActiveCount();
    }

    int getQueueCapacity() {
        if (threadPoolExecutor == null) {
            return 0;
        }
        return threadPoolExecutor.getQueue().size()
                + threadPoolExecutor.getQueue().remainingCapacity();
    }

    @Override
    protected void doClose() {
        // Executor lifecycle is owned by caller.
    }
}
