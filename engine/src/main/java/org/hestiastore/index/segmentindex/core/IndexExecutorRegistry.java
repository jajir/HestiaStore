package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Owns executor lifecycle for SegmentIndex subsystems.
 * <p>
 * All executor pools are created eagerly at construction time and are shared
 * for the full lifetime of this registry. Getter methods reject access once the
 * registry is closed.
 * </p>
 */
final class IndexExecutorRegistry extends AbstractCloseableResource {

    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;
    private static final String ARG_INDEX_CONFIGURATION = "indexConfiguration";
    private static final String ARG_INDEX_MAINTENANCE_THREADS = "indexMaintenanceThreads";
    private static final String ARG_SPLIT_MAINTENANCE_THREADS = "splitMaintenanceThreads";
    private static final String ARG_STABLE_SEGMENT_MAINTENANCE_THREADS = "numberOfStableSegmentMaintenanceThreads";
    private static final String ARG_REGISTRY_MAINTENANCE_THREADS = "registryMaintenanceThreads";
    private static final String ARG_EXECUTOR = "executor";
    private static final String ARG_INDEX_NAME = "indexName";
    private static final String THREAD_NAME_PREFIX_INDEX_MAINTENANCE = "index-maintenance-";
    private static final String THREAD_NAME_PREFIX_SPLIT_MAINTENANCE = "split-maintenance-";
    private static final String THREAD_NAME_PREFIX_SPLIT_POLICY = "split-policy-";
    private static final String THREAD_NAME_PREFIX_STABLE_SEGMENT_MAINTENANCE = "stable-segment-maintenance-";
    private static final String THREAD_NAME_PREFIX_REGISTRY_MAINTENANCE = "registry-maintenance-";
    private static final String MESSAGE_ALREADY_CLOSED = "IndexExecutorRegistry already closed";

    private final ThreadPoolExecutor indexMaintenanceExecutorDelegate;
    private final ExecutorService indexMaintenanceExecutor;
    private final ThreadPoolExecutor splitMaintenanceExecutorDelegate;
    private final ExecutorService splitMaintenanceExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final ExecutorService stableSegmentMaintenanceExecutor;
    private final ExecutorService registryMaintenanceExecutor;

    /**
     * Creates a registry using thread settings from index configuration.
     *
     * @param indexConfiguration index configuration
     */
    IndexExecutorRegistry(final IndexConfiguration<?, ?> indexConfiguration) {
        final IndexConfiguration<?, ?> conf = Vldtn
                .requireNonNull(indexConfiguration, ARG_INDEX_CONFIGURATION);
        this.indexMaintenanceExecutorDelegate = createIndexMaintenanceExecutor(
                conf);
        this.indexMaintenanceExecutor = wrapWithIndexContextIfEnabled(conf,
                indexMaintenanceExecutorDelegate);
        this.splitMaintenanceExecutorDelegate = createSplitMaintenanceExecutor(
                conf);
        this.splitMaintenanceExecutor = wrapWithIndexContextIfEnabled(conf,
                splitMaintenanceExecutorDelegate);
        this.splitPolicyScheduler = createSplitPolicyScheduler();
        this.stableSegmentMaintenanceExecutor = wrapWithIndexContextIfEnabled(
                conf, createStableSegmentMaintenanceExecutor(conf));
        this.registryMaintenanceExecutor = wrapWithIndexContextIfEnabled(conf,
                createRegistryMaintenanceExecutor(conf));
    }

    /**
     * Returns shared stable-segment maintenance executor.
     *
     * @return stable-segment maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getStableSegmentMaintenanceExecutor() {
        checkNotClosed();
        return stableSegmentMaintenanceExecutor;
    }

    /**
     * Returns shared index-maintenance executor.
     *
     * @return index-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getIndexMaintenanceExecutor() {
        checkNotClosed();
        return indexMaintenanceExecutor;
    }

    /**
     * Returns shared split-maintenance executor.
     *
     * @return split-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getSplitMaintenanceExecutor() {
        checkNotClosed();
        return splitMaintenanceExecutor;
    }

    /**
     * Returns shared split-policy scheduler.
     *
     * @return split-policy scheduler
     * @throws IllegalStateException when registry has already been closed
     */
    ScheduledExecutorService getSplitPolicyScheduler() {
        checkNotClosed();
        return splitPolicyScheduler;
    }

    /**
     * Returns shared registry-maintenance executor.
     *
     * @return registry-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getRegistryMaintenanceExecutor() {
        checkNotClosed();
        return registryMaintenanceExecutor;
    }

    int indexMaintenanceQueueSize() {
        checkNotClosed();
        return indexMaintenanceExecutorDelegate.getQueue().size();
    }

    int indexMaintenanceQueueCapacity() {
        checkNotClosed();
        return queueCapacity(indexMaintenanceExecutorDelegate);
    }

    int splitMaintenanceQueueSize() {
        checkNotClosed();
        return splitMaintenanceExecutorDelegate.getQueue().size();
    }

    int splitMaintenanceQueueCapacity() {
        checkNotClosed();
        return queueCapacity(splitMaintenanceExecutorDelegate);
    }

    private static ThreadPoolExecutor createIndexMaintenanceExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int indexMaintenanceThreads = Vldtn
                .requireGreaterThanZero(
                        Vldtn.requireNonNull(
                                Vldtn.requireNonNull(conf,
                                        ARG_INDEX_CONFIGURATION)
                                        .getNumberOfIndexMaintenanceThreads(),
                                ARG_INDEX_MAINTENANCE_THREADS),
                        ARG_INDEX_MAINTENANCE_THREADS);
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                indexMaintenanceThreads * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return new ThreadPoolExecutor(indexMaintenanceThreads,
                indexMaintenanceThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), runnable -> {
                    final Thread thread = new Thread(runnable,
                            THREAD_NAME_PREFIX_INDEX_MAINTENANCE
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    private static ThreadPoolExecutor createStableSegmentMaintenanceExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int stableSegmentMaintenanceThreads = Vldtn.requireGreaterThanZero(
                Vldtn.requireNonNull(
                        Vldtn.requireNonNull(conf, ARG_INDEX_CONFIGURATION)
                                .getNumberOfStableSegmentMaintenanceThreads(),
                        ARG_STABLE_SEGMENT_MAINTENANCE_THREADS),
                ARG_STABLE_SEGMENT_MAINTENANCE_THREADS);
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                stableSegmentMaintenanceThreads * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return new ThreadPoolExecutor(stableSegmentMaintenanceThreads,
                stableSegmentMaintenanceThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), runnable -> {
                    final Thread thread = new Thread(runnable,
                            THREAD_NAME_PREFIX_STABLE_SEGMENT_MAINTENANCE
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static ThreadPoolExecutor createSplitMaintenanceExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int splitMaintenanceThreads = Vldtn
                .requireGreaterThanZero(
                        Vldtn.requireNonNull(
                                Vldtn.requireNonNull(conf,
                                        ARG_INDEX_CONFIGURATION)
                                        .getNumberOfIndexMaintenanceThreads(),
                                ARG_SPLIT_MAINTENANCE_THREADS),
                        ARG_SPLIT_MAINTENANCE_THREADS);
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                splitMaintenanceThreads * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return new ThreadPoolExecutor(splitMaintenanceThreads,
                splitMaintenanceThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), runnable -> {
                    final Thread thread = new Thread(runnable,
                            THREAD_NAME_PREFIX_SPLIT_MAINTENANCE
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    private static ExecutorService createRegistryMaintenanceExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int registryMaintenanceThreads = Vldtn
                .requireGreaterThanZero(
                        Vldtn.requireNonNull(
                                Vldtn.requireNonNull(conf,
                                        ARG_INDEX_CONFIGURATION)
                                        .getNumberOfRegistryLifecycleThreads(),
                                ARG_REGISTRY_MAINTENANCE_THREADS),
                        ARG_REGISTRY_MAINTENANCE_THREADS);
        return createFixedDaemonExecutor(registryMaintenanceThreads,
                THREAD_NAME_PREFIX_REGISTRY_MAINTENANCE);
    }

    private static ScheduledExecutorService createSplitPolicyScheduler() {
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable,
                    THREAD_NAME_PREFIX_SPLIT_POLICY
                            + threadCounter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    private static ExecutorService wrapWithIndexContextIfEnabled(
            final IndexConfiguration<?, ?> conf,
            final ExecutorService executor) {
        final IndexConfiguration<?, ?> configuration = Vldtn
                .requireNonNull(conf, ARG_INDEX_CONFIGURATION);
        final ExecutorService delegate = Vldtn.requireNonNull(executor,
                ARG_EXECUTOR);
        if (!Boolean.TRUE.equals(configuration.isContextLoggingEnabled())) {
            return delegate;
        }
        return new IndexNameMdcExecutorService(Vldtn.requireNotBlank(
                configuration.getIndexName(), ARG_INDEX_NAME), delegate);
    }

    private static int queueCapacity(final ThreadPoolExecutor executor) {
        return executor.getQueue().size()
                + executor.getQueue().remainingCapacity();
    }

    private static ExecutorService createFixedDaemonExecutor(
            final int threadCount, final String threadNamePrefix) {
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return Executors.newFixedThreadPool(threadCount, runnable -> {
            final Thread thread = new Thread(runnable,
                    threadNamePrefix + threadCounter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    protected void doClose() {
        RuntimeException failure = null;
        failure = shutdownAndAwait(indexMaintenanceExecutor, failure);
        failure = shutdownAndAwait(splitMaintenanceExecutor, failure);
        failure = shutdownAndAwait(splitPolicyScheduler, failure);
        failure = shutdownAndAwait(stableSegmentMaintenanceExecutor, failure);
        failure = shutdownAndAwait(registryMaintenanceExecutor, failure);
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Shuts down executor and waits until termination, aggregating failures.
     *
     * @param executor executor to shut down
     * @param failure  previously observed failure (if any)
     * @return original or aggregated failure, {@code null} when shutdown
     *         completed cleanly
     */
    private RuntimeException shutdownAndAwait(final ExecutorService executor,
            final RuntimeException failure) {
        if (executor == null) {
            return failure;
        }
        RuntimeException nextFailure = failure;
        executor.shutdown();
        boolean interrupted = false;
        try {
            while (!executor.isTerminated()) {
                awaitTerminationStep(executor);
            }
        } catch (final InterruptedException e) {
            interrupted = true;
            executor.shutdownNow();
        } catch (final RuntimeException e) {
            if (nextFailure == null) {
                nextFailure = e;
            } else {
                nextFailure.addSuppressed(e);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return nextFailure;
    }

    private void awaitTerminationStep(final ExecutorService executor)
            throws InterruptedException {
        executor.awaitTermination(1, TimeUnit.SECONDS);
    }

    private void checkNotClosed() {
        if (wasClosed()) {
            throw new IllegalStateException(MESSAGE_ALREADY_CLOSED);
        }
    }
}
