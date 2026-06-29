package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;

/**
 * Internal bridge between public runtime ownership and segment-index bootstrap.
 */
public interface SegmentIndexRuntimeHandle {

    /**
     * Creates the executor registry used by one index instance.
     *
     * @param indexName index name used for logging context and index-owned thread
     *        names
     * @param contextLoggingEnabled true when MDC context wrapping is enabled
     * @param indexMaintenanceThreads index-maintenance worker count
     * @param registryMaintenanceThreads registry-maintenance worker count
     * @param shutdownTimeoutMillis executor shutdown timeout
     * @return executor registry for one index instance
     */
    ExecutorRegistry createExecutorRegistry(
            String indexName,
            boolean contextLoggingEnabled,
            int indexMaintenanceThreads,
            int registryMaintenanceThreads,
            int shutdownTimeoutMillis);

    /**
     * Returns the runtime-wide thread-name prefix.
     *
     * @return thread-name prefix
     */
    String threadNamePrefix();

    /**
     * Releases the runtime when it is owned by the index.
     */
    void close();
}
