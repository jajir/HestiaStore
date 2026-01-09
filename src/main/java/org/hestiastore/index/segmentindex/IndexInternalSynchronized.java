package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;

public class IndexInternalSynchronized<K, V> extends SegmentIndexImpl<K, V> {

    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;
    private static final String CPU_THREAD_NAME_PREFIX = "cpuPool";

    private final ThreadPoolExecutor executor;
    private final ThreadLocal<Boolean> inExecutorThread = ThreadLocal
            .withInitial(() -> Boolean.FALSE);
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public IndexInternalSynchronized(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf);
        final Integer threadsConf = conf.getNumberOfThreads();
        final int threads = (threadsConf == null || threadsConf < 1) ? 1
                : threadsConf.intValue();
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                threads * QUEUE_CAPACITY_MULTIPLIER);
        this.executor = new ThreadPoolExecutor(threads, threads, 0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                namedThreadFactory(CPU_THREAD_NAME_PREFIX),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static ThreadFactory namedThreadFactory(final String prefix) {
        final AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setName(prefix + "-" + counter.getAndIncrement());
            return thread;
        };
    }

    private boolean isRunningOnIndexExecutorThread() {
        return Boolean.TRUE.equals(inExecutorThread.get());
    }

    private void ensureAcceptingTasks() {
        if (closing.get() || executor.isShutdown()) {
            throw new IllegalStateException("Index is closing.");
        }
    }

    private <T> T executeWithRead(final Callable<T> task) {
        return executeOnExecutor(task);
    }

    private <T> T executeWithWrite(final Callable<T> task) {
        return executeOnExecutor(task);
    }

    private <T> T executeOnExecutor(final Callable<T> task) {
        if (isRunningOnIndexExecutorThread()) {
            return callUnchecked(task);
        }
        ensureAcceptingTasks();
        try {
            return executor.submit(() -> {
                final boolean previous = isRunningOnIndexExecutorThread();
                inExecutorThread.set(Boolean.TRUE);
                try {
                    return task.call();
                } finally {
                    inExecutorThread.set(previous);
                }
            }).get();
        } catch (final RejectedExecutionException e) {
            throw new IllegalStateException(
                    "Operation rejected while index is closing.", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Operation interrupted while waiting for executor", e);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(
                    "Operation failed in executor: " + cause.getMessage(),
                    cause);
        } catch (final Exception e) {
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(
                    "Operation failed: " + e.getMessage(), e);
        }
    }

    private <T> T callUnchecked(final Callable<T> task) {
        try {
            return task.call();
        } catch (final Exception e) {
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(
                    "Operation failed: " + e.getMessage(), e);
        }
    }

    private <T> CompletionStage<T> submitAsync(final Executor executor,
            final Callable<T> task) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        if (closing.get() || this.executor.isShutdown()) {
            future.completeExceptionally(
                    new IllegalStateException("Index is closing."));
            return future;
        }
        try {
            executor.execute(() -> {
                final boolean previous = isRunningOnIndexExecutorThread();
                inExecutorThread.set(Boolean.TRUE);
                try {
                    future.complete(task.call());
                } catch (final Throwable t) {
                    future.completeExceptionally(t);
                } finally {
                    inExecutorThread.set(previous);
                }
            });
        } catch (final RejectedExecutionException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    protected void doClose() {
        if (isRunningOnIndexExecutorThread()) {
            throw new IllegalStateException(
                    "close() must not be called from index executor thread.");
        }
        closing.set(true);
        executor.shutdown();
        awaitExecutorTermination();
        super.doClose();
    }

    private void awaitExecutorTermination() {
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

    @Override
    public void put(final K key, final V value) {
        executeWithWrite(() -> {
            super.put(key, value);
            return null;
        });
    }

    @Override
    public V get(final K key) {
        return executeWithRead(() -> super.get(key));
    }

    @Override
    public void delete(final K key) {
        executeWithWrite(() -> {
            super.delete(key);
            return null;
        });
    }

    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return submitAsync(executor, () -> {
            super.put(key, value);
            return null;
        });
    }

    @Override
    public CompletionStage<V> getAsync(final K key) {
        return submitAsync(executor, () -> super.get(key));
    }

    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return submitAsync(executor, () -> {
            super.delete(key);
            return null;
        });
    }

    @Override
    public void compact() {
        executeWithWrite(() -> {
            invalidateSegmentIterators();
            super.compact();
            return null;
        });
    }

    @Override
    public void compactAndWait() {
        executeWithWrite(() -> {
            invalidateSegmentIterators();
            super.compactAndWait();
            return null;
        });
    }

    @Override
    public Stream<Entry<K, V>> getStream(SegmentWindow segmentWindow) {
        final List<Entry<K, V>> snapshot = executeWithRead(() -> {
            getIndexState().tryPerformOperation();
            final EntryIterator<K, V> iterator = openSegmentIterator(
                    segmentWindow);
            try {
                final List<Entry<K, V>> out = new ArrayList<>();
                while (iterator.hasNext()) {
                    out.add(iterator.next());
                }
                return out;
            } finally {
                iterator.close();
            }
        });
        return snapshot.stream();
    }

    @Override
    public void flush() {
        executeWithWrite(() -> {
            invalidateSegmentIterators();
            super.flush();
            return null;
        });

    }

    @Override
    public void flushAndWait() {
        executeWithWrite(() -> {
            invalidateSegmentIterators();
            super.flushAndWait();
            return null;
        });
    }

    @Override
    public void checkAndRepairConsistency() {
        executeWithWrite(() -> {
            invalidateSegmentIterators();
            super.checkAndRepairConsistency();
            return null;
        });
    }

}
