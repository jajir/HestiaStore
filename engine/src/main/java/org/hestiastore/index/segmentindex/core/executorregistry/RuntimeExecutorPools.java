package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Owns process-level executor pools shared by one or more segment indexes.
 */
public final class RuntimeExecutorPools extends AbstractCloseableResource {

    private static final String ARG_THREAD_NAME_PREFIX = "threadNamePrefix";
    private static final String ARG_SEGMENT_MAINTENANCE_THREADS = "segmentMaintenanceThreads";
    private static final String ARG_SPLIT_MAINTENANCE_THREADS = "splitMaintenanceThreads";
    private static final String ARG_SHUTDOWN_TIMEOUT_MILLIS = "shutdownTimeoutMillis";
    private static final String POOL_NAME_SEGMENT_MAINTENANCE = "segment-maintenance";
    private static final String POOL_NAME_SPLIT_MAINTENANCE = "split-maintenance";

    private final String threadNamePrefix;
    private final ObservedThreadPool segmentMaintenanceThreadPool;
    private final ObservedThreadPool splitMaintenanceThreadPool;
    private final int shutdownTimeoutMillis;

    RuntimeExecutorPools(
            final String threadNamePrefix,
            final ObservedThreadPool segmentMaintenanceThreadPool,
            final ObservedThreadPool splitMaintenanceThreadPool,
            final int shutdownTimeoutMillis) {
        this.threadNamePrefix = Vldtn.requireNotBlank(threadNamePrefix,
                ARG_THREAD_NAME_PREFIX);
        this.segmentMaintenanceThreadPool = Vldtn.requireNonNull(
                segmentMaintenanceThreadPool, "segmentMaintenanceThreadPool");
        this.splitMaintenanceThreadPool = Vldtn.requireNonNull(
                splitMaintenanceThreadPool, "splitMaintenanceThreadPool");
        this.shutdownTimeoutMillis = Vldtn.requireGreaterThanZero(
                shutdownTimeoutMillis, ARG_SHUTDOWN_TIMEOUT_MILLIS);
    }

    /**
     * Creates shared runtime executor pools.
     *
     * @param threadNamePrefix common thread-name prefix
     * @param segmentMaintenanceThreads segment-maintenance worker count
     * @param splitMaintenanceThreads split-maintenance worker count
     * @param shutdownTimeoutMillis shutdown timeout in milliseconds
     * @return shared runtime executor pools
     */
    public static RuntimeExecutorPools create(
            final String threadNamePrefix,
            final int segmentMaintenanceThreads,
            final int splitMaintenanceThreads,
            final int shutdownTimeoutMillis) {
        final String validatedThreadNamePrefix = Vldtn.requireNotBlank(
                threadNamePrefix, ARG_THREAD_NAME_PREFIX);
        final ObservedThreadPoolFactory threadPoolFactory =
                new ObservedThreadPoolFactory();
        return new RuntimeExecutorPools(
                validatedThreadNamePrefix,
                threadPoolFactory.createCallerRunsPool(
                        segmentMaintenanceThreads,
                        ARG_SEGMENT_MAINTENANCE_THREADS,
                        poolThreadNamePrefix(validatedThreadNamePrefix,
                                POOL_NAME_SEGMENT_MAINTENANCE)),
                threadPoolFactory.createAbortingPool(splitMaintenanceThreads,
                        ARG_SPLIT_MAINTENANCE_THREADS,
                        poolThreadNamePrefix(validatedThreadNamePrefix,
                                POOL_NAME_SPLIT_MAINTENANCE)),
                shutdownTimeoutMillis);
    }

    String threadNamePrefix() {
        return threadNamePrefix;
    }

    ObservedThreadPool segmentMaintenanceThreadPool() {
        ensureOpen();
        return segmentMaintenanceThreadPool;
    }

    ObservedThreadPool splitMaintenanceThreadPool() {
        ensureOpen();
        return splitMaintenanceThreadPool;
    }

    private void ensureOpen() {
        if (wasClosed()) {
            throw new IllegalStateException(
                    "RuntimeExecutorPools already closed");
        }
    }

    @Override
    protected void doClose() {
        RuntimeException failure = null;
        failure = ExecutorShutdown.shutdownAndAwait(
                POOL_NAME_SPLIT_MAINTENANCE,
                splitMaintenanceThreadPool.executor(), shutdownTimeoutMillis,
                failure);
        failure = ExecutorShutdown.shutdownAndAwait(
                POOL_NAME_SEGMENT_MAINTENANCE,
                segmentMaintenanceThreadPool.executor(), shutdownTimeoutMillis,
                failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static String poolThreadNamePrefix(final String prefix,
            final String poolName) {
        return prefix + "-" + poolName + "-";
    }
}
