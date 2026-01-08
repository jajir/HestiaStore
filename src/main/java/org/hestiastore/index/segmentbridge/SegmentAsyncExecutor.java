package org.hestiastore.index.segmentbridge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Shared executor for segment maintenance tasks.
 */
public final class SegmentAsyncExecutor extends AbstractCloseableResource {

    private final ExecutorService executor;

    public SegmentAsyncExecutor(final int threads,
            final String threadNamePrefix) {
        Vldtn.requireGreaterThanZero(threads, "threads");
        this.executor = Executors.newFixedThreadPool(threads,
                namedThreadFactory(threadNamePrefix));
    }

    public ExecutorService getExecutor() {
        return executor;
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
