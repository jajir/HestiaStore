package org.hestiastore.index.segmentindex;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorStreamer;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.log.Log;
import org.hestiastore.index.log.LoggedKey;

public class IndexInternalSynchronized<K, V> extends SegmentIndexImpl<K, V> {

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final ExecutorService executor;

    public IndexInternalSynchronized(final Directory directory,
        final TypeDescriptor<K> keyTypeDescriptor,
        final TypeDescriptor<V> valueTypeDescriptor,
        final IndexConfiguration<K, V> conf, final Log<K, V> log) {
    super(directory, keyTypeDescriptor, valueTypeDescriptor, conf, log);
    final Integer threadsConf = conf.getNumberOfThreads();
    final int threads = (threadsConf == null || threadsConf < 1) ? 1
            : threadsConf.intValue();
    this.executor = Executors.newFixedThreadPool(threads);
}

    private <T> T executeWithRead(final Callable<T> task) {
        try {
            return executor.submit(() -> {
                readLock.lock();
                try {
                    return task.call();
                } finally {
                    readLock.unlock();
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
        try {
            return executor.submit(() -> {
                writeLock.lock();
                try {
                    return task.call();
                } finally {
                    writeLock.unlock();
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
    public EntryIteratorStreamer<LoggedKey<K>, V> getLogStreamer() {
        return executeWithRead(() -> super.getLogStreamer());
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
