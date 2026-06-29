package org.hestiastore.index.segmentindex;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.RuntimeExecutorPools;

/**
 * Process-level runtime resources that can be shared by multiple segment
 * indexes.
 * <p>
 * Callers that pass a runtime to a segment index remain responsible for closing
 * the runtime after all indexes using it have been closed.
 * </p>
 */
public final class HestiaStoreRuntime extends AbstractCloseableResource {

    private final String threadNamePrefix;
    private final RuntimeExecutorPools executorPools;

    HestiaStoreRuntime(final String threadNamePrefix,
            final RuntimeExecutorPools executorPools) {
        this.threadNamePrefix = Vldtn.requireNotBlank(threadNamePrefix,
                "threadNamePrefix");
        this.executorPools = Vldtn.requireNonNull(executorPools,
                "executorPools");
    }

    /**
     * Creates a new runtime builder.
     *
     * @return runtime builder
     */
    public static HestiaStoreRuntimeBuilder builder() {
        return new HestiaStoreRuntimeBuilder();
    }

    ExecutorRegistry createExecutorRegistry(
            final String indexName,
            final boolean contextLoggingEnabled,
            final int indexMaintenanceThreads,
            final int registryMaintenanceThreads,
            final int shutdownTimeoutMillis) {
        ensureOpen();
        return ExecutorRegistry.create(indexName, contextLoggingEnabled,
                indexMaintenanceThreads, executorPools,
                registryMaintenanceThreads, shutdownTimeoutMillis);
    }

    /**
     * Returns the runtime-wide thread-name prefix.
     *
     * @return thread-name prefix
     */
    String threadNamePrefix() {
        ensureOpen();
        return threadNamePrefix;
    }

    private void ensureOpen() {
        if (wasClosed()) {
            throw new IllegalStateException("HestiaStoreRuntime already closed");
        }
    }

    @Override
    protected void doClose() {
        executorPools.close();
    }
}
