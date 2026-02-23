package org.hestiastore.index.segmentindex;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Owns executor lifecycle for SegmentIndex subsystems.
 * <p>
 * All executor pools are created eagerly at construction time and are shared
 * for the full lifetime of this registry. Getter methods reject access once the
 * registry is closed.
 * </p>
 */
final class IndexExecutorRegistry extends AbstractCloseableResource {

    private static final AtomicInteger IO_THREAD_COUNTER = new AtomicInteger(1);
    private static final AtomicInteger SEGMENT_THREAD_COUNTER = new AtomicInteger(
            1);
    private static final AtomicInteger SEGMENT_MAINTENANCE_THREAD_COUNTER = new AtomicInteger(
            1);
    private static final AtomicInteger REGISTRY_MAINTENANCE_THREAD_COUNTER = new AtomicInteger(
            1);

    private final ExecutorService ioExecutor;
    private final ExecutorService segmentExecutor;
    private final ExecutorService segmentMaintenanceExecutor;
    private final ExecutorService registryMaintenanceExecutor;

    /**
     * Creates a registry with predefined non-IO pool sizes.
     * <p>
     * Kept as compatibility constructor for existing call-sites that only
     * choose IO pool size.
     * </p>
     *
     * @param ioThreads IO pool size
     */
    IndexExecutorRegistry(final int ioThreads) {
        this(ioThreads, 4, 3, 3);
    }

    /**
     * Creates a registry using thread settings from index configuration.
     *
     * @param indexConfiguration index configuration
     */
    IndexExecutorRegistry(
            final IndexConfiguration<?, ?> indexConfiguration) {
        this(Vldtn.requireNonNull(indexConfiguration, "indexConfiguration")
                .getNumberOfIoThreads(),
                indexConfiguration.getNumberOfSegmentIndexMaintenanceThreads(),
                indexConfiguration.getIndexWorkerThreadCount(),
                indexConfiguration.getNumberOfRegistryLifecycleThreads());
    }

    /**
     * Creates a registry with explicit pool sizes for each executor group.
     *
     * @param numberOfIoThreads          IO pool size
     * @param segmentMaintenanceThreads  segment-maintenance pool size
     * @param segmentThreads             segment pool size
     * @param registryMaintenanceThreads registry-maintenance pool size
     */
    IndexExecutorRegistry(final int numberOfIoThreads,
            final int segmentMaintenanceThreads, final int segmentThreads,
            final int registryMaintenanceThreads) {
        Vldtn.requireGreaterThanZero(numberOfIoThreads, "ioThreads");
        Vldtn.requireGreaterThanZero(segmentThreads, "segmentThreads");
        Vldtn.requireGreaterThanZero(segmentMaintenanceThreads,
                "segmentMaintenanceThreads");
        Vldtn.requireGreaterThanZero(registryMaintenanceThreads,
                "registryMaintenanceThreads");
        this.ioExecutor = newExecutor(numberOfIoThreads, "index-io-",
                IO_THREAD_COUNTER);
        this.segmentExecutor = newExecutor(segmentThreads, "segment-",
                SEGMENT_THREAD_COUNTER);
        this.segmentMaintenanceExecutor = newExecutor(segmentMaintenanceThreads,
                "segment-maintenance-", SEGMENT_MAINTENANCE_THREAD_COUNTER);
        this.registryMaintenanceExecutor = newExecutor(
                registryMaintenanceThreads, "registry-maintenance-",
                REGISTRY_MAINTENANCE_THREAD_COUNTER);
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
     * Returns shared segment executor.
     *
     * @return segment executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getSegmentExecutor() {
        checkNotClosed();
        return segmentExecutor;
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

    private static ExecutorService newExecutor(final int threadCount,
            final String threadNamePrefix, final AtomicInteger threadCounter) {
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
        failure = shutdownAndAwait(segmentExecutor, failure);
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
                try {
                    executor.awaitTermination(1, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }
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

    private void checkNotClosed() {
        if (wasClosed()) {
            throw new IllegalStateException(
                    "IndexExecutorRegistry already closed");
        }
    }
}
