package org.hestiastore.index.segmentindex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Shared executor for segment maintenance tasks.
 */
public final class SegmentAsyncExecutor extends AbstractCloseableResource {

    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;

    private final ThreadPoolExecutor executor;
    private final int queueCapacity;

    /**
     * Creates a shared executor for segment maintenance tasks.
     *
     * @param threads number of threads in the pool
     * @param threadNamePrefix thread name prefix
     */
    public SegmentAsyncExecutor(final int threads,
            final String threadNamePrefix) {
        Vldtn.requireGreaterThanZero(threads, "threads");
        this.queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                threads * QUEUE_CAPACITY_MULTIPLIER);
        this.executor = new ThreadPoolExecutor(threads, threads, 0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                namedThreadFactory(threadNamePrefix),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * Returns the underlying executor service.
     *
     * @return executor service
     */
    public ExecutorService getExecutor() {
        return executor;
    }

    int getQueueSize() {
        return executor.getQueue().size();
    }

    int getActiveCount() {
        return executor.getActiveCount();
    }

    int getQueueCapacity() {
        return queueCapacity;
    }

    @Override
    protected void doClose() {
        executor.shutdown();
        awaitTermination();
    }

    private void awaitTermination() {
        boolean interrupted = false;
        try {
            while (!executor.isTerminated()) {
                try {
                    executor.awaitTermination(1, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static ThreadFactory namedThreadFactory(final String prefix) {
        Vldtn.requireNonNull(prefix, "prefix");
        if (prefix.isEmpty()) {
            throw new IllegalArgumentException(
                    "Property 'prefix' must not be empty.");
        }
        final AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setName(prefix + "-" + counter.getAndIncrement());
            return thread;
        };
    }
}
