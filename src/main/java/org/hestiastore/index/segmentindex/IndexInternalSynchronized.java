package org.hestiastore.index.segmentindex;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(
            true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final ExecutorService executor;
    private final ThreadLocal<Boolean> inExecutorThread = ThreadLocal
            .withInitial(() -> Boolean.FALSE);
    private final boolean contextLoggingEnabled;
    private final String indexName;

    public IndexInternalSynchronized(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf, final Log<K, V> log) {
        super(directory, keyTypeDescriptor, valueTypeDescriptor, conf, log);
        final Integer threadsConf = conf.getNumberOfThreads();
        final int threads = (threadsConf == null || threadsConf < 1) ? 1
                : threadsConf.intValue();
        this.executor = threads == 1 ? Executors.newSingleThreadExecutor()
                : Executors.newFixedThreadPool(threads);
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
        executeWithWrite(() -> {
            super.doClose();
            return null;
        });
        executor.shutdown();
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
