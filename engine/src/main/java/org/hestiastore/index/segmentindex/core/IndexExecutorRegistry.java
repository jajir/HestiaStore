package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private final ExecutorService ioExecutor;
    private final ExecutorService indexMaintenanceExecutor;
    private final ExecutorService segmentMaintenanceExecutor;
    private final ExecutorService registryMaintenanceExecutor;

    /**
     * Creates a registry using thread settings from index configuration.
     *
     * @param indexConfiguration index configuration
     */
    IndexExecutorRegistry(final IndexConfiguration<?, ?> indexConfiguration) {
        final IndexConfiguration<?, ?> conf = Vldtn.requireNonNull(
                indexConfiguration, "indexConfiguration");
        this.ioExecutor = wrapWithIndexContextIfEnabled(conf,
                createIoExecutor(conf));
        this.indexMaintenanceExecutor = wrapWithIndexContextIfEnabled(conf,
                createIndexMaintenanceExecutor(conf));
        this.segmentMaintenanceExecutor = wrapWithIndexContextIfEnabled(conf,
                createSegmentMaintenanceExecutor(conf));
        this.registryMaintenanceExecutor = wrapWithIndexContextIfEnabled(conf,
                createRegistryMaintenanceExecutor(conf));
    }

    /**
     * Returns shared IO executor.
     *
     * @return IO executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getIoExecutor() {
        checkNotClosed();
        return ioExecutor;
    }

    /**
     * Returns shared segment-maintenance executor.
     *
     * @return segment-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getSegmentMaintenanceExecutor() {
        checkNotClosed();
        return segmentMaintenanceExecutor;
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
     * Returns shared registry-maintenance executor.
     *
     * @return registry-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getRegistryMaintenanceExecutor() {
        checkNotClosed();
        return registryMaintenanceExecutor;
    }

    private static ExecutorService createIoExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int ioThreads = Vldtn.requireGreaterThanZero(
                Vldtn.requireNonNull(
                        Vldtn.requireNonNull(conf, "indexConfiguration")
                                .getNumberOfIoThreads(),
                        "ioThreads"),
                "ioThreads");
        return createFixedDaemonExecutor(ioThreads, "index-io-");
    }

    private static ExecutorService createIndexMaintenanceExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int indexMaintenanceThreads = Vldtn.requireGreaterThanZero(
                Vldtn.requireNonNull(
                        Vldtn.requireNonNull(conf, "indexConfiguration")
                                .getNumberOfIndexMaintenanceThreads(),
                        "indexMaintenanceThreads"),
                "indexMaintenanceThreads");
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                indexMaintenanceThreads * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return new ThreadPoolExecutor(indexMaintenanceThreads,
                indexMaintenanceThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), runnable -> {
                    final Thread thread = new Thread(runnable,
                            "index-maintenance-"
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    private static ExecutorService createSegmentMaintenanceExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int segmentMaintenanceThreads = Vldtn.requireGreaterThanZero(
                Vldtn.requireNonNull(
                        Vldtn.requireNonNull(conf, "indexConfiguration")
                                .getNumberOfSegmentIndexMaintenanceThreads(),
                        "segmentMaintenanceThreads"),
                "segmentMaintenanceThreads");
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                segmentMaintenanceThreads * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return new ThreadPoolExecutor(segmentMaintenanceThreads,
                segmentMaintenanceThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), runnable -> {
                    final Thread thread = new Thread(runnable,
                            "segment-maintenance-"
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static ExecutorService createRegistryMaintenanceExecutor(
            final IndexConfiguration<?, ?> conf) {
        final int registryMaintenanceThreads = Vldtn.requireGreaterThanZero(
                Vldtn.requireNonNull(
                        Vldtn.requireNonNull(conf, "indexConfiguration")
                                .getNumberOfRegistryLifecycleThreads(),
                        "registryMaintenanceThreads"),
                "registryMaintenanceThreads");
        return createFixedDaemonExecutor(registryMaintenanceThreads,
                "registry-maintenance-");
    }

    private static ExecutorService wrapWithIndexContextIfEnabled(
            final IndexConfiguration<?, ?> conf,
            final ExecutorService executor) {
        final IndexConfiguration<?, ?> configuration = Vldtn
                .requireNonNull(conf, "indexConfiguration");
        final ExecutorService delegate = Vldtn.requireNonNull(executor,
                "executor");
        if (!Boolean.TRUE.equals(configuration.isContextLoggingEnabled())) {
            return delegate;
        }
        return new IndexNameMdcExecutorService(
                Vldtn.requireNonNull(configuration.getIndexName(), "indexName"),
                delegate);
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
        failure = shutdownAndAwait(ioExecutor, failure);
        failure = shutdownAndAwait(indexMaintenanceExecutor, failure);
        failure = shutdownAndAwait(segmentMaintenanceExecutor, failure);
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
            throw new IllegalStateException(
                    "IndexExecutorRegistry already closed");
        }
    }
}
