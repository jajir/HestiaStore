package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.Vldtn;

/**
 * Builds executor registries from explicit executor settings.
 */
public final class ExecutorRegistryBuilder {

    private static final String ARG_INDEX_MAINTENANCE_THREADS = "indexMaintenanceThreads";
    private static final String ARG_SPLIT_MAINTENANCE_THREADS = "splitMaintenanceThreads";
    private static final String ARG_SEGMENT_MAINTENANCE_THREADS = "numberOfSegmentMaintenanceThreads";
    private static final String ARG_REGISTRY_MAINTENANCE_THREADS = "registryMaintenanceThreads";
    private static final String ARG_INDEX_NAME = "indexName";
    private static final String THREAD_NAME_PREFIX_INDEX_MAINTENANCE = "index-maintenance-";
    private static final String THREAD_NAME_PREFIX_SPLIT_MAINTENANCE = "split-maintenance-";
    private static final String THREAD_NAME_PREFIX_SPLIT_POLICY = "split-policy-";
    private static final String THREAD_NAME_PREFIX_SEGMENT_MAINTENANCE = "segment-maintenance-";
    private static final String THREAD_NAME_PREFIX_REGISTRY_MAINTENANCE = "registry-maintenance-";

    private Integer segmentMaintenanceThreads;
    private Integer indexMaintenanceThreads;
    private Integer splitMaintenanceThreads;
    private Integer registryMaintenanceThreads;
    private boolean contextLoggingEnabled;
    private String indexName;

    ExecutorRegistryBuilder() {
    }

    /**
     * Sets segment-maintenance worker count.
     *
     * @param threads segment-maintenance worker count
     * @return this builder
     */
    public ExecutorRegistryBuilder withSegmentMaintenanceThreads(
            final Integer threads) {
        this.segmentMaintenanceThreads = threads;
        return this;
    }

    /**
     * Sets index-maintenance worker count.
     *
     * @param threads index-maintenance worker count
     * @return this builder
     */
    public ExecutorRegistryBuilder withIndexMaintenanceThreads(
            final Integer threads) {
        this.indexMaintenanceThreads = threads;
        return this;
    }

    /**
     * Sets split-maintenance worker count.
     *
     * @param threads split-maintenance worker count
     * @return this builder
     */
    public ExecutorRegistryBuilder withSplitMaintenanceThreads(
            final Integer threads) {
        this.splitMaintenanceThreads = threads;
        return this;
    }

    /**
     * Sets registry-maintenance worker count.
     *
     * @param threads registry-maintenance worker count
     * @return this builder
     */
    public ExecutorRegistryBuilder withRegistryMaintenanceThreads(
            final Integer threads) {
        this.registryMaintenanceThreads = threads;
        return this;
    }

    /**
     * Enables or disables MDC context propagation for registry executors.
     *
     * @param enabled true when index-name MDC context should be propagated
     * @return this builder
     */
    public ExecutorRegistryBuilder withContextLoggingEnabled(
            final boolean enabled) {
        this.contextLoggingEnabled = enabled;
        return this;
    }

    /**
     * Sets index name used when context logging is enabled.
     *
     * @param value index name
     * @return this builder
     */
    public ExecutorRegistryBuilder withIndexName(final String value) {
        this.indexName = value;
        return this;
    }

    /**
     * Builds executor registry.
     *
     * @return executor registry
     */
    public ExecutorRegistry build() {
        final ObservedThreadPoolFactory threadPoolFactory =
                new ObservedThreadPoolFactory();
        final ExecutorContextDecorator contextDecorator =
                new ExecutorContextDecorator(contextLoggingEnabled,
                        contextIndexName());
        final int indexMaintenanceThreadCount = indexMaintenanceThreads();
        final int splitMaintenanceThreadCount = splitMaintenanceThreads();
        final int segmentMaintenanceThreadCount = segmentMaintenanceThreads();
        final int registryMaintenanceThreadCount =
                registryMaintenanceThreads();
        final ObservedThreadPool indexMaintenanceThreadPool =
                createIndexMaintenanceThreadPool(threadPoolFactory,
                        indexMaintenanceThreadCount);
        final ObservedThreadPool splitMaintenanceThreadPool =
                createSplitMaintenanceThreadPool(threadPoolFactory,
                        splitMaintenanceThreadCount);
        final ObservedThreadPool stableSegmentMaintenanceThreadPool =
                createStableSegmentMaintenanceThreadPool(threadPoolFactory,
                        segmentMaintenanceThreadCount);
        return new ExecutorRegistryImpl(
                new ExecutorTopology(
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
                new ExecutorRuntimeMonitor(indexMaintenanceThreadPool,
                        splitMaintenanceThreadPool,
                        stableSegmentMaintenanceThreadPool));
    }

    private String contextIndexName() {
        if (!contextLoggingEnabled) {
            return indexName;
        }
        return Vldtn.requireNotBlank(indexName, ARG_INDEX_NAME);
    }

    private ObservedThreadPool createIndexMaintenanceThreadPool(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int threadCount) {
        return threadPoolFactory.createAbortingPool(
                threadCount,
                ARG_INDEX_MAINTENANCE_THREADS,
                THREAD_NAME_PREFIX_INDEX_MAINTENANCE);
    }

    private ObservedThreadPool createStableSegmentMaintenanceThreadPool(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int threadCount) {
        return threadPoolFactory.createCallerRunsPool(
                threadCount,
                ARG_SEGMENT_MAINTENANCE_THREADS,
                THREAD_NAME_PREFIX_SEGMENT_MAINTENANCE);
    }

    private ObservedThreadPool createSplitMaintenanceThreadPool(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int threadCount) {
        return threadPoolFactory.createAbortingPool(
                threadCount,
                ARG_SPLIT_MAINTENANCE_THREADS,
                THREAD_NAME_PREFIX_SPLIT_MAINTENANCE);
    }

    private ExecutorService contextAwareObservedExecutor(
            final ExecutorContextDecorator contextDecorator,
            final ObservedThreadPool threadPool) {
        return contextDecorator.decorate(threadPool.executor());
    }

    private ExecutorService createRegistryMaintenanceExecutor(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int registryMaintenanceThreadCount) {
        return createFixedDaemonExecutor(threadPoolFactory,
                registryMaintenanceThreadCount,
                THREAD_NAME_PREFIX_REGISTRY_MAINTENANCE);
    }

    private ScheduledExecutorService createSplitPolicyScheduler(
            final ObservedThreadPoolFactory threadPoolFactory) {
        return Executors.newSingleThreadScheduledExecutor(
                threadPoolFactory.daemonThreadFactory(
                        THREAD_NAME_PREFIX_SPLIT_POLICY));
    }

    private ExecutorService createFixedDaemonExecutor(
            final ObservedThreadPoolFactory threadPoolFactory,
            final int threadCount, final String threadNamePrefix) {
        return Executors.newFixedThreadPool(threadCount,
                threadPoolFactory.daemonThreadFactory(threadNamePrefix));
    }

    private int registryMaintenanceThreads() {
        return ObservedThreadPoolFactory.configuredThreadCount(
                registryMaintenanceThreads, ARG_REGISTRY_MAINTENANCE_THREADS);
    }

    private int indexMaintenanceThreads() {
        return ObservedThreadPoolFactory.configuredThreadCount(
                indexMaintenanceThreads, ARG_INDEX_MAINTENANCE_THREADS);
    }

    private int splitMaintenanceThreads() {
        return ObservedThreadPoolFactory.configuredThreadCount(
                splitMaintenanceThreads, ARG_SPLIT_MAINTENANCE_THREADS);
    }

    private int segmentMaintenanceThreads() {
        return ObservedThreadPoolFactory.configuredThreadCount(
                segmentMaintenanceThreads, ARG_SEGMENT_MAINTENANCE_THREADS);
    }
}
