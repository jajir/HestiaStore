package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.ExecutorService;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Split-maintenance executor facade backed by {@link IndexExecutorRegistry}.
 */
public final class SplitAsyncExecutor extends AbstractCloseableResource {

    private final IndexExecutorRegistry executorRegistry;

    /**
     * Creates split maintenance executor adapter backed by
     * {@link IndexExecutorRegistry}.
     *
     * @param executorRegistry shared index executor registry
     */
    public SplitAsyncExecutor(final IndexExecutorRegistry executorRegistry) {
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
    }

    /**
     * Returns the underlying executor service.
     *
     * @return executor service
     */
    public ExecutorService getExecutor() {
        return executorRegistry.getSegmentExecutor();
    }

    @Override
    protected void doClose() {
        // Executor lifecycle is owned by IndexExecutorRegistry.
    }
}
