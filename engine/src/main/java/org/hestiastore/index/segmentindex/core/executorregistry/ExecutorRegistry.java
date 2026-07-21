package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Owns executor lifecycle for SegmentIndex subsystems.
 */
public final class ExecutorRegistry extends AbstractCloseableResource {

    private static final String MESSAGE_ALREADY_CLOSED = "ExecutorRegistry already closed";
    private static final String ARG_INDEX_MAINTENANCE_THREADS = "indexMaintenanceThreads";
    private static final String ARG_REGISTRY_MAINTENANCE_THREADS = "registryMaintenanceThreads";
    private static final String ARG_SHUTDOWN_TIMEOUT_MILLIS = "shutdownTimeoutMillis";
    private static final String ARG_INDEX_NAME = "indexName";
    private static final String POOL_NAME_INDEX_MAINTENANCE = "index-maintenance";
    private static final String POOL_NAME_SPLIT_POLICY = "split-policy";
    private static final String POOL_NAME_REGISTRY_MAINTENANCE = "registry-maintenance";
    private static final String POOL_NAME_WAL_APPEND = "wal-append";

    private final ExecutorTopology topology;
    private final ExecutorRuntimeMonitor runtimeMonitor;
    private final ThreadFactory walAppendThreadFactory;

    /**
     * Creates one index-scoped registry and its WAL worker factory.
     *
     * @param topology executor topology owned or borrowed by this registry
     * @param runtimeMonitor executor metrics source
     * @param walAppendThreadFactory named daemon factory for the WAL worker
     */
    ExecutorRegistry(final ExecutorTopology topology,
            final ExecutorRuntimeMonitor runtimeMonitor,
            final ThreadFactory walAppendThreadFactory) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.runtimeMonitor = Vldtn.requireNonNull(runtimeMonitor,
                "runtimeMonitor");
        this.walAppendThreadFactory = Vldtn.requireNonNull(
                walAppendThreadFactory, "walAppendThreadFactory");
    }

    /**
     * Creates an executor registry for one index runtime.
     *
     * @param indexName index name used for logging context and index-owned thread
     *        names
     * @param contextLoggingEnabled true when MDC context wrapping is enabled
     * @param indexMaintenanceThreads index-maintenance worker count
     * @param runtimeExecutorPools runtime-owned shared executor pools
     * @param registryMaintenanceThreads registry-maintenance worker count
     * @param shutdownTimeoutMillis executor shutdown timeout
     * @return executor registry
     */
    public static ExecutorRegistry create(
            final String indexName,
            final boolean contextLoggingEnabled,
            final int indexMaintenanceThreads,
            final RuntimeExecutorPools runtimeExecutorPools,
            final int registryMaintenanceThreads,
            final int shutdownTimeoutMillis) {
        final ObservedThreadPoolFactory threadPoolFactory =
                new ObservedThreadPoolFactory();
        final ExecutorContextDecorator contextDecorator =
                new ExecutorContextDecorator(contextLoggingEnabled,
                        contextIndexName(indexName, contextLoggingEnabled));
        final RuntimeExecutorPools validatedRuntimeExecutorPools = Vldtn
                .requireNonNull(runtimeExecutorPools, "runtimeExecutorPools");
        final String validatedIndexName = Vldtn.requireNotBlank(indexName,
                ARG_INDEX_NAME);
        final String threadNamePrefix =
                validatedRuntimeExecutorPools.threadNamePrefix();
        final ObservedThreadPool indexMaintenanceThreadPool =
                createIndexMaintenanceThreadPool(threadPoolFactory,
                        indexMaintenanceThreads,
                        poolThreadNamePrefix(threadNamePrefix,
                                validatedIndexName,
                                POOL_NAME_INDEX_MAINTENANCE));
        final ObservedThreadPool splitMaintenanceThreadPool =
                validatedRuntimeExecutorPools.splitMaintenanceThreadPool();
        final ObservedThreadPool stableSegmentMaintenanceThreadPool =
                validatedRuntimeExecutorPools.segmentMaintenanceThreadPool();
        return new ExecutorRegistry(
                new ExecutorTopology(
                        contextAwareObservedExecutor(contextDecorator,
                                indexMaintenanceThreadPool),
                        contextAwareObservedExecutor(contextDecorator,
                                splitMaintenanceThreadPool),
                        createSplitPolicyScheduler(threadPoolFactory,
                                poolThreadNamePrefix(threadNamePrefix,
                                        validatedIndexName,
                                        POOL_NAME_SPLIT_POLICY)),
                        contextAwareObservedExecutor(contextDecorator,
                                stableSegmentMaintenanceThreadPool),
                        contextDecorator.decorate(
                                createRegistryMaintenanceExecutor(
                                        threadPoolFactory,
                                        registryMaintenanceThreads,
                                        poolThreadNamePrefix(threadNamePrefix,
                                                validatedIndexName,
                                                POOL_NAME_REGISTRY_MAINTENANCE))),
                        Vldtn.requireGreaterThanZero(shutdownTimeoutMillis,
                                ARG_SHUTDOWN_TIMEOUT_MILLIS)),
                new ExecutorRuntimeMonitor(indexMaintenanceThreadPool,
                        splitMaintenanceThreadPool,
                        stableSegmentMaintenanceThreadPool),
                threadPoolFactory.daemonThreadFactory(
                        poolThreadNamePrefix(threadNamePrefix,
                                validatedIndexName,
                                POOL_NAME_WAL_APPEND)));
    }

    /**
     * Returns shared segment maintenance executor.
     *
     * @return segment maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getStableSegmentMaintenanceExecutor() {
        ensureOpen();
        return topology.stableSegmentMaintenanceExecutor();
    }

    /**
     * Returns shared index-maintenance executor.
     *
     * @return index-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getIndexMaintenanceExecutor() {
        ensureOpen();
        return topology.indexMaintenanceExecutor();
    }

    /**
     * Returns shared split-maintenance executor.
     *
     * @return split-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getSplitMaintenanceExecutor() {
        ensureOpen();
        return topology.splitMaintenanceExecutor();
    }

    /**
     * Returns shared split-policy scheduler.
     *
     * @return split-policy scheduler
     * @throws IllegalStateException when registry has already been closed
     */
    public ScheduledExecutorService getSplitPolicyScheduler() {
        ensureOpen();
        return topology.splitPolicyScheduler();
    }

    /**
     * Returns shared registry-maintenance executor.
     *
     * @return registry-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getRegistryMaintenanceExecutor() {
        ensureOpen();
        return topology.registryMaintenanceExecutor();
    }

    /**
     * Returns the factory for the index-owned WAL append worker.
     *
     * <p>
     * The WAL runtime owns the resulting thread lifecycle so it can drain its
     * queue and join the worker before closing WAL storage.
     * </p>
     *
     * @return named daemon thread factory for the WAL append worker
     * @throws IllegalStateException when registry has already been closed
     */
    public ThreadFactory getWalAppendThreadFactory() {
        ensureOpen();
        return walAppendThreadFactory;
    }

    /**
     * Captures current executor runtime stats.
     *
     * @return executor registry stats snapshot
     */
    public ExecutorRegistryStats statsSnapshot() {
        return runtimeMonitor.statsSnapshot();
    }

    private void ensureOpen() {
        if (wasClosed()) {
            throw new IllegalStateException(MESSAGE_ALREADY_CLOSED);
        }
    }

    @Override
    protected void doClose() {
        final RuntimeException failure = topology.shutdownExecutorsInCloseOrder();
        if (failure != null) {
            throw failure;
        }
    }

    private static String contextIndexName(final String indexName,
            final boolean contextLoggingEnabled) {
        if (!contextLoggingEnabled) {
            return indexName;
        }
        return Vldtn.requireNotBlank(indexName, ARG_INDEX_NAME);
    }

    private static ObservedThreadPool createIndexMaintenanceThreadPool(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int threadCount,
            final String threadNamePrefix) {
        return threadPoolFactory.createAbortingPool(threadCount,
                ARG_INDEX_MAINTENANCE_THREADS,
                threadNamePrefix);
    }

    private static ExecutorService contextAwareObservedExecutor(
            final ExecutorContextDecorator contextDecorator,
            final ObservedThreadPool threadPool) {
        return contextDecorator.decorate(threadPool.executor());
    }

    private static ExecutorService createRegistryMaintenanceExecutor(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int registryMaintenanceThreadCount,
            final String threadNamePrefix) {
        return Executors.newFixedThreadPool(
                ObservedThreadPoolFactory.configuredThreadCount(
                        registryMaintenanceThreadCount,
                        ARG_REGISTRY_MAINTENANCE_THREADS),
                threadPoolFactory.daemonThreadFactory(threadNamePrefix));
    }

    private static ScheduledExecutorService createSplitPolicyScheduler(
            final ObservedThreadPoolFactory threadPoolFactory,
            final String threadNamePrefix) {
        final ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(1,
                        threadPoolFactory.daemonThreadFactory(threadNamePrefix));
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private static String poolThreadNamePrefix(final String prefix,
            final String indexName, final String poolName) {
        return prefix + "-" + indexName + "-" + poolName + "-";
    }

}
