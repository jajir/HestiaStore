package org.hestiastore.index.segmentindex;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Owns index IO executor lifecycle.
 */
final class IndexExecutorRegistry extends AbstractCloseableResource {

    private static final AtomicInteger IO_THREAD_COUNTER = new AtomicInteger(1);
    private static final AtomicInteger SEGMENT_MAINTENANCE_THREAD_COUNTER = new AtomicInteger(
            1);
    private static final AtomicInteger REGISTRY_MAINTENANCE_THREAD_COUNTER = new AtomicInteger(
            1);

    private final Object monitor = new Object();
    private final int ioThreads;
    private final int segmentMaintenanceThreads;
    private final int registryMaintenanceThreads;
    private ExecutorService ioExecutor;
    private ExecutorService segmentMaintenanceExecutor;
    private ExecutorService registryMaintenanceExecutor;

    IndexExecutorRegistry(final int ioThreads) {
        // FIXME remove it
        this(ioThreads, 4, 3, 3);
    }

    IndexExecutorRegistry(final int numberOfIoThreads,
            final int segmentMaintenanceThreads, final int segmentThreads,
            final int registryMaintenanceThreads) {
        Vldtn.requireGreaterThanZero(numberOfIoThreads, "ioThreads");
        Vldtn.requireGreaterThanZero(segmentMaintenanceThreads,
                "segmentMaintenanceThreads");
        Vldtn.requireGreaterThanZero(registryMaintenanceThreads,
                "registryMaintenanceThreads");
        this.ioThreads = numberOfIoThreads;
        this.segmentMaintenanceThreads = segmentMaintenanceThreads;
        this.registryMaintenanceThreads = registryMaintenanceThreads;
    }

    ExecutorService getIoExecutor() {
        synchronized (monitor) {
            checkNotClosed();
            if (ioExecutor == null) {
                ioExecutor = newExecutor(ioThreads, "index-io-",
                        IO_THREAD_COUNTER);
            }
            return ioExecutor;
        }
    }

    ExecutorService getSegmentMaintenanceExecutor() {
        synchronized (monitor) {
            checkNotClosed();
            if (segmentMaintenanceExecutor == null) {
                segmentMaintenanceExecutor = newExecutor(
                        segmentMaintenanceThreads, "segment-maintenance-",
                        SEGMENT_MAINTENANCE_THREAD_COUNTER);
            }
            return segmentMaintenanceExecutor;
        }
    }

    ExecutorService getRegistryMaintenanceExecutor() {
        synchronized (monitor) {
            checkNotClosed();
            if (registryMaintenanceExecutor == null) {
                registryMaintenanceExecutor = newExecutor(
                        registryMaintenanceThreads, "registry-maintenance-",
                        REGISTRY_MAINTENANCE_THREAD_COUNTER);
            }
            return registryMaintenanceExecutor;
        }
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
        final ExecutorService io;
        final ExecutorService segmentMaintenance;
        final ExecutorService registryMaintenance;
        synchronized (monitor) {
            io = ioExecutor;
            segmentMaintenance = segmentMaintenanceExecutor;
            registryMaintenance = registryMaintenanceExecutor;
            ioExecutor = null;
            segmentMaintenanceExecutor = null;
            registryMaintenanceExecutor = null;
        }
        RuntimeException failure = null;
        failure = shutdownAndAwait(io, failure);
        failure = shutdownAndAwait(segmentMaintenance, failure);
        failure = shutdownAndAwait(registryMaintenance, failure);
        if (failure != null) {
            throw failure;
        }
    }

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
