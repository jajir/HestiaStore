package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorRuntimeAccess;

/**
 * Owns executor lifecycle for SegmentIndex subsystems.
 * <p>
 * Hot-path observed maintenance pools are created eagerly at construction
 * time. Less frequently used support executors are created lazily on first
 * access and then shared for the rest of the registry lifetime. Getter methods
 * reject access once the registry is closed.
 * </p>
 */
public final class IndexExecutorRegistry extends AbstractCloseableResource {

    private static final String ARG_INDEX_CONFIGURATION = "indexConfiguration";
    private static final String ARG_INDEX_MAINTENANCE_THREADS = "indexMaintenanceThreads";
    private static final String ARG_SPLIT_MAINTENANCE_THREADS = "splitMaintenanceThreads";
    private static final String ARG_SEGMENT_MAINTENANCE_THREADS = "numberOfSegmentMaintenanceThreads";
    private static final String ARG_REGISTRY_MAINTENANCE_THREADS = "registryMaintenanceThreads";
    private static final String MESSAGE_ALREADY_CLOSED = "IndexExecutorRegistry already closed";
    private static final String THREAD_NAME_PREFIX_INDEX_MAINTENANCE = "index-maintenance-";
    private static final String THREAD_NAME_PREFIX_SPLIT_MAINTENANCE = "split-maintenance-";
    private static final String THREAD_NAME_PREFIX_SPLIT_POLICY = "split-policy-";
    private static final String THREAD_NAME_PREFIX_SEGMENT_MAINTENANCE = "segment-maintenance-";
    private static final String THREAD_NAME_PREFIX_REGISTRY_MAINTENANCE = "registry-maintenance-";

    private final IndexExecutorTopology topology;
    private final IndexExecutorRuntimeMonitor runtimeMonitor;

    /**
     * Creates a registry using thread settings from index configuration.
     *
     * @param indexConfiguration index configuration
     */
    public IndexExecutorRegistry(
            final IndexConfiguration<?, ?> indexConfiguration) {
        final ExecutorAssembly assembly = createAssembly(
                requireConfiguration(indexConfiguration));
        this.topology = assembly.topology;
        this.runtimeMonitor = assembly.runtimeMonitor;
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

    public IndexExecutorRuntimeAccess runtimeSnapshot() {
        ensureOpen();
        return runtimeMonitor.runtimeSnapshot();
    }

    private static IndexConfiguration<?, ?> requireConfiguration(
            final IndexConfiguration<?, ?> indexConfiguration) {
        return Vldtn.requireNonNull(indexConfiguration,
                ARG_INDEX_CONFIGURATION);
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

    private static ExecutorAssembly createAssembly(
            final IndexConfiguration<?, ?> configuration) {
        final ObservedThreadPoolFactory threadPoolFactory =
                new ObservedThreadPoolFactory(configuration);
        final IndexExecutorContextDecorator contextDecorator =
                new IndexExecutorContextDecorator(configuration);
        final ObservedThreadPool indexMaintenanceThreadPool =
                createIndexMaintenanceThreadPool(threadPoolFactory);
        final ObservedThreadPool splitMaintenanceThreadPool =
                createSplitMaintenanceThreadPool(threadPoolFactory);
        final ObservedThreadPool stableSegmentMaintenanceThreadPool =
                createStableSegmentMaintenanceThreadPool(threadPoolFactory);
        final int registryMaintenanceThreadCount = configuredThreadCount(
                threadPoolFactory,
                IndexConfiguration::getNumberOfRegistryLifecycleThreads,
                ARG_REGISTRY_MAINTENANCE_THREADS);
        return new ExecutorAssembly(
                new IndexExecutorTopology(
                        contextAwareObservedExecutor(contextDecorator,
                                indexMaintenanceThreadPool),
                        contextAwareObservedExecutor(contextDecorator,
                                splitMaintenanceThreadPool),
                        new LazyExecutorReference<>(
                                () -> createSplitPolicyScheduler(
                                        threadPoolFactory)),
                        contextAwareObservedExecutor(contextDecorator,
                                stableSegmentMaintenanceThreadPool),
                        new LazyExecutorReference<>(
                                () -> contextDecorator.decorate(
                                        createRegistryMaintenanceExecutor(
                                                threadPoolFactory,
                                                registryMaintenanceThreadCount)))),
                new IndexExecutorRuntimeMonitor(indexMaintenanceThreadPool,
                        splitMaintenanceThreadPool,
                        stableSegmentMaintenanceThreadPool));
    }

    private static ObservedThreadPool createIndexMaintenanceThreadPool(
            final ObservedThreadPoolFactory threadPoolFactory) {
        return threadPoolFactory.createAbortingPool(
                IndexConfiguration::getNumberOfIndexMaintenanceThreads,
                ARG_INDEX_MAINTENANCE_THREADS,
                THREAD_NAME_PREFIX_INDEX_MAINTENANCE);
    }

    private static ObservedThreadPool createStableSegmentMaintenanceThreadPool(
            final ObservedThreadPoolFactory threadPoolFactory) {
        return threadPoolFactory.createCallerRunsPool(
                IndexConfiguration::getNumberOfSegmentMaintenanceThreads,
                ARG_SEGMENT_MAINTENANCE_THREADS,
                THREAD_NAME_PREFIX_SEGMENT_MAINTENANCE);
    }

    private static ObservedThreadPool createSplitMaintenanceThreadPool(
            final ObservedThreadPoolFactory threadPoolFactory) {
        return threadPoolFactory.createAbortingPool(
                IndexConfiguration::getNumberOfIndexMaintenanceThreads,
                ARG_SPLIT_MAINTENANCE_THREADS,
                THREAD_NAME_PREFIX_SPLIT_MAINTENANCE);
    }

    private static ExecutorService contextAwareObservedExecutor(
            final IndexExecutorContextDecorator contextDecorator,
            final ObservedThreadPool threadPool) {
        return contextDecorator.decorate(threadPool.executor());
    }

    private static ExecutorService createRegistryMaintenanceExecutor(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int threadCount) {
        return createFixedDaemonExecutor(threadPoolFactory,
                threadCount,
                THREAD_NAME_PREFIX_REGISTRY_MAINTENANCE);
    }

    private static ScheduledExecutorService createSplitPolicyScheduler(
            final ObservedThreadPoolFactory threadPoolFactory) {
        return Executors.newSingleThreadScheduledExecutor(
                threadPoolFactory.daemonThreadFactory(
                        THREAD_NAME_PREFIX_SPLIT_POLICY));
    }

    private static ExecutorService createFixedDaemonExecutor(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int threadCount, final String threadNamePrefix) {
        return Executors.newFixedThreadPool(threadCount,
                threadPoolFactory.daemonThreadFactory(threadNamePrefix));
    }

    private static int configuredThreadCount(
            final ObservedThreadPoolFactory threadPoolFactory,
            final Function<IndexConfiguration<?, ?>, Integer> threadCountSupplier,
            final String threadCountArgumentName) {
        return threadPoolFactory.configuredThreadCount(threadCountSupplier,
                threadCountArgumentName);
    }

    private static final class ExecutorAssembly {

        private final IndexExecutorTopology topology;
        private final IndexExecutorRuntimeMonitor runtimeMonitor;

        private ExecutorAssembly(final IndexExecutorTopology topology,
                final IndexExecutorRuntimeMonitor runtimeMonitor) {
            this.topology = Vldtn.requireNonNull(topology, "topology");
            this.runtimeMonitor = Vldtn.requireNonNull(runtimeMonitor,
                    "runtimeMonitor");
        }
    }

}
