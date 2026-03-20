package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Dedicated async executor for index API operations.
 */
final class IndexAsyncExecutor extends AbstractCloseableResource {

    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;
    private static final String ARG_INDEX_CONFIGURATION = "indexConfiguration";
    private static final String ARG_INDEX_NAME = "indexName";
    private static final String ARG_INDEX_WORKER_THREAD_COUNT = "indexWorkerThreadCount";
    private static final String THREAD_NAME_PREFIX_INDEX_WORKER = "index-worker-";

    private final ExecutorService executor;

    IndexAsyncExecutor(final IndexConfiguration<?, ?> conf) {
        this.executor = createExecutor(conf);
    }

    <T> CompletionStage<T> runAsync(final Supplier<T> task) {
        final Supplier<T> nonNullTask = Vldtn.requireNonNull(task, "task");
        try {
            return CompletableFuture.supplyAsync(
                    () -> IndexAsyncExecutionContext.runInAsyncContext(
                            nonNullTask),
                    executor);
        } catch (final RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    protected void doClose() {
        if (IndexAsyncExecutionContext.isAsyncOperationActive()) {
            throw new IllegalStateException(
                    "close() must not be called from an async index operation.");
        }
        executor.shutdown();
    }

    private static ExecutorService createExecutor(
            final IndexConfiguration<?, ?> conf) {
        final IndexConfiguration<?, ?> configuration = Vldtn.requireNonNull(conf,
                ARG_INDEX_CONFIGURATION);
        final int workerThreadCount = Vldtn.requireGreaterThanZero(
                Vldtn.requireNonNull(configuration.getIndexWorkerThreadCount(),
                        ARG_INDEX_WORKER_THREAD_COUNT),
                ARG_INDEX_WORKER_THREAD_COUNT);
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                workerThreadCount * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger threadCounter = new AtomicInteger(1);
        final ExecutorService delegate = new ThreadPoolExecutor(
                workerThreadCount, workerThreadCount, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueCapacity),
                runnable -> {
                    final Thread thread = new Thread(runnable,
                            THREAD_NAME_PREFIX_INDEX_WORKER
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        if (!Boolean.TRUE.equals(configuration.isContextLoggingEnabled())) {
            return delegate;
        }
        return new IndexNameMdcExecutorService(
                Vldtn.requireNotBlank(configuration.getIndexName(),
                        ARG_INDEX_NAME),
                delegate);
    }
}
