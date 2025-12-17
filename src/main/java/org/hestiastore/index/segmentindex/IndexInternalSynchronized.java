package org.hestiastore.index.segmentindex;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
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

    private final ReentrantLock lock = new ReentrantLock();
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

    private <T> T executeWithLock(final Callable<T> task) {
        try {
            return executor.submit(() -> {
                lock.lock();
                try {
                    return task.call();
                } finally {
                    lock.unlock();
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
        executeWithLock(() -> {
            super.doClose();
            return null;
        });
        executor.shutdown();
    }

    @Override
    public void put(final K key, final V value) {
        executeWithLock(() -> {
            super.put(key, value);
            return null;
        });
    }

    @Override
    public V get(final K key) {
        return executeWithLock(() -> super.get(key));
    }

    @Override
    public void delete(final K key) {
        executeWithLock(() -> {
            super.delete(key);
            return null;
        });
    }

    @Override
    public void compact() {
        executeWithLock(() -> {
            super.compact();
            return null;
        });
    }

    @Override
    public Stream<Entry<K, V>> getStream(SegmentWindow segmentWindow) {
        lock.lock();
        try {
            indexState.tryPerformOperation();
            final EntryIterator<K, V> iterator = openSegmentIterator(
                    segmentWindow);
            final EntryIterator<K, V> synchronizedIterator = new EntryIteratorSynchronized<>(
                    iterator, lock);
            final EntryIteratorToSpliterator<K, V> spliterator = new EntryIteratorToSpliterator<K, V>(
                    synchronizedIterator, keyTypeDescriptor);
            return StreamSupport.stream(spliterator, false).onClose(() -> {
                iterator.close();
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public EntryIteratorStreamer<LoggedKey<K>, V> getLogStreamer() {
        return executeWithLock(() -> super.getLogStreamer());
    }

    @Override
    public void flush() {
        executeWithLock(() -> {
            super.flush();
            return null;
        });

    }

    @Override
    public void checkAndRepairConsistency() {
        executeWithLock(() -> {
            super.checkAndRepairConsistency();
            return null;
        });
    }

}
