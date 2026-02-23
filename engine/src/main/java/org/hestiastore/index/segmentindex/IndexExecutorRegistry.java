package org.hestiastore.index.segmentindex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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

    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;

    private final ExecutorService ioExecutor;
    private final ThreadPoolExecutor segmentExecutor;
    private final ThreadPoolExecutor segmentMaintenanceExecutor;
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
    IndexExecutorRegistry(final IndexConfiguration<?, ?> indexConfiguration) {
        this(Vldtn.requireNonNull(indexConfiguration, "indexConfiguration")
                .getNumberOfIoThreads(),
                indexConfiguration.getNumberOfSegmentIndexMaintenanceThreads(),
                indexConfiguration.getNumberOfIndexMaintenanceThreads(),
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
        this.ioExecutor = newExecutor(numberOfIoThreads, "index-io-");
        final int segmentExecutorQueueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                segmentThreads * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger segmentThreadCounter = new AtomicInteger(1);
        this.segmentExecutor = new ThreadPoolExecutor(segmentThreads,
                segmentThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(segmentExecutorQueueCapacity),
                runnable -> {
                    final Thread thread = new Thread(runnable, "segment-"
                            + segmentThreadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        final int segmentMaintenanceExecutorQueueCapacity = Math.max(
                MIN_QUEUE_CAPACITY,
                segmentMaintenanceThreads * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger segmentMaintenanceThreadCounter = new AtomicInteger(
                1);
        this.segmentMaintenanceExecutor = new ThreadPoolExecutor(
                segmentMaintenanceThreads, segmentMaintenanceThreads, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(
                        segmentMaintenanceExecutorQueueCapacity),
                runnable -> {
                    final Thread thread = new Thread(runnable,
                            "segment-maintenance-"
                                    + segmentMaintenanceThreadCounter
                                            .getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.CallerRunsPolicy());
        this.registryMaintenanceExecutor = newExecutor(
                registryMaintenanceThreads, "registry-maintenance-");
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
            final String threadNamePrefix) {
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
