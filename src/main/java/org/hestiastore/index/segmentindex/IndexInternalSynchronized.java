package org.hestiastore.index.segmentindex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.log.Log;
import org.slf4j.MDC;

public class IndexInternalSynchronized<K, V> extends SegmentIndexImpl<K, V> {

    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(
            true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final ThreadPoolExecutor executor;
    private final ThreadLocal<Boolean> inExecutorThread = ThreadLocal
            .withInitial(() -> Boolean.FALSE);
    private final boolean contextLoggingEnabled;
    private final String indexName;
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public IndexInternalSynchronized(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf, final Log<K, V> log) {
        super(directory, keyTypeDescriptor, valueTypeDescriptor, conf, log);
        final Integer threadsConf = conf.getNumberOfThreads();
        final int threads = (threadsConf == null || threadsConf < 1) ? 1
                : threadsConf.intValue();
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                threads * QUEUE_CAPACITY_MULTIPLIER);
        this.executor = new ThreadPoolExecutor(threads, threads, 0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.contextLoggingEnabled = Boolean.TRUE
                .equals(conf.isContextLoggingEnabled());
        this.indexName = conf.getIndexName() == null ? "" : conf.getIndexName();
    }

    private void setContext() {
        if (!contextLoggingEnabled) {
            return;
        }
        MDC.put(INDEX_NAME_MDC_KEY, indexName);
    }

    private void restoreContext(final String previousValue) {
        if (!contextLoggingEnabled) {
            return;
        }
        if (previousValue == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
        } else {
            MDC.put(INDEX_NAME_MDC_KEY, previousValue);
        }
    }

    private boolean isRunningOnIndexExecutorThread() {
        return Boolean.TRUE.equals(inExecutorThread.get());
    }

    private void ensureAcceptingTasks() {
        if (closing.get() || executor.isShutdown()) {
            throw new IllegalStateException("Index is closing.");
        }
    }

    private <T> T executeWithLock(final Lock lock, final Callable<T> task)
            throws Exception {
        final String previousIndexName = contextLoggingEnabled
                ? MDC.get(INDEX_NAME_MDC_KEY)
                : null;
        setContext();
        lock.lock();
        try {
            return task.call();
        } finally {
            lock.unlock();
            restoreContext(previousIndexName);
        }
    }

    private <T> T executeWithRead(final Callable<T> task) {
        if (isRunningOnIndexExecutorThread()) {
            try {
                return executeWithLock(readLock, task);
            } catch (final Exception e) {
                if (e instanceof RuntimeException re) {
                    throw re;
                }
                throw new IllegalStateException(
                        "Operation failed: " + e.getMessage(), e);
            }
        }
        ensureAcceptingTasks();
        try {
            return executor.submit(() -> {
                final boolean previous = isRunningOnIndexExecutorThread();
                inExecutorThread.set(Boolean.TRUE);
                try {
                    return executeWithLock(readLock, task);
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
        }
    }

    private <T> T executeWithWrite(final Callable<T> task) {
        if (isRunningOnIndexExecutorThread()) {
            try {
                return executeWithLock(writeLock, task);
            } catch (final Exception e) {
                if (e instanceof RuntimeException re) {
                    throw re;
                }
                throw new IllegalStateException(
                        "Operation failed: " + e.getMessage(), e);
            }
        }
        ensureAcceptingTasks();
        try {
            return executor.submit(() -> {
                final boolean previous = isRunningOnIndexExecutorThread();
                inExecutorThread.set(Boolean.TRUE);
                try {
                    return executeWithLock(writeLock, task);
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
        }
    }

    private <T> CompletionStage<T> submitAsync(final Executor executor,
            final Lock lock, final Callable<T> task) {
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
                    future.complete(executeWithLock(lock, task));
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
        try {
            executeWithLock(writeLock, () -> {
                super.doClose();
                return null;
            });
        } catch (final Exception e) {
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(
                    "Failed to close index: " + e.getMessage(), e);
        }
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
        return submitAsync(executor, writeLock, () -> {
            super.put(key, value);
            return null;
        });
    }

    @Override
    public CompletionStage<V> getAsync(final K key) {
        return submitAsync(executor, readLock, () -> super.get(key));
    }

    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return submitAsync(executor, writeLock, () -> {
            super.delete(key);
            return null;
        });
    }

    @Override
    public void compact() {
        executeWithWrite(() -> {
            super.compact();
            return null;
        });
    }

    @Override
    public Stream<Entry<K, V>> getStream(SegmentWindow segmentWindow) {
        return executeWithRead(() -> {
            indexState.tryPerformOperation();
            final EntryIterator<K, V> iterator = openSegmentIterator(
                    segmentWindow);
            final EntryIterator<K, V> synchronizedIterator = new EntryIteratorSynchronized<>(
                    iterator, readLock);
            final EntryIteratorToSpliterator<K, V> spliterator = new EntryIteratorToSpliterator<K, V>(
                    synchronizedIterator, keyTypeDescriptor);
            return StreamSupport.stream(spliterator, false).onClose(() -> {
                iterator.close();
            });
        });
    }

    @Override
    public void flush() {
        executeWithWrite(() -> {
            super.flush();
            return null;
        });

    }

    @Override
    public void checkAndRepairConsistency() {
        executeWithWrite(() -> {
            super.checkAndRepairConsistency();
            return null;
        });
    }

}
