package org.hestiastore.index.segment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;

/**
 * Serializes access to a single {@link Segment} instance using a dedicated
 * {@link ReentrantReadWriteLock}. Read operations can run concurrently, while
 * writers/compaction are exclusive and block readers for their duration.
 */
public class SegmentSynchronizationAdapter<K, V>
        extends AbstractCloseableResource implements Segment<K, V> {

    private final Segment<K, V> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(
            true);
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final AtomicBoolean compactionScheduled = new AtomicBoolean(false);
    private final AtomicBoolean compactionRerun = new AtomicBoolean(false);
    // Keep compaction off the write path while still honoring the write lock.
    private final ExecutorService compactionExecutor;

    public SegmentSynchronizationAdapter(final Segment<K, V> delegate) {
        this.delegate = delegate;
        final SegmentId id = delegate == null ? null : delegate.getId();
        final String prefix = id == null ? "segmentCompaction"
                : "segmentCompaction-" + id.getName();
        this.compactionExecutor = Executors
                .newSingleThreadExecutor(namedThreadFactory(prefix));
        if (delegate instanceof SegmentImpl<K, V> impl) {
            impl.setCompactionExecutor(this::scheduleCompaction);
        }
    }

    private static ThreadFactory namedThreadFactory(final String prefix) {
        final AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setName(prefix + "-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    private void scheduleCompaction(final SegmentImpl<K, V> segment) {
        if (segment.wasClosed() || wasClosed()
                || compactionExecutor.isShutdown()) {
            return;
        }
        if (lock.isWriteLockedByCurrentThread()) {
            if (!compactionScheduled.compareAndSet(false, true)) {
                compactionRerun.set(true);
                return;
            }
            try {
                runCompaction(segment);
                while (compactionRerun.getAndSet(false)) {
                    runCompaction(segment);
                }
            } finally {
                compactionScheduled.set(false);
            }
            return;
        }
        if (!compactionScheduled.compareAndSet(false, true)) {
            compactionRerun.set(true);
            return;
        }
        try {
            compactionExecutor.execute(() -> {
                try {
                    runCompaction(segment);
                    while (compactionRerun.getAndSet(false)) {
                        runCompaction(segment);
                    }
                } finally {
                    compactionScheduled.set(false);
                }
            });
        } catch (final RejectedExecutionException e) {
            compactionScheduled.set(false);
        }
    }

    private void runCompaction(final SegmentImpl<K, V> segment) {
        if (segment.wasClosed() || wasClosed()) {
            return;
        }
        executeWithWriteLock(() -> {
            if (!segment.wasClosed()) {
                segment.compact();
            }
            return null;
        });
    }

    @Override
    public SegmentStats getStats() {
        return delegate.getStats();
    }

    @Override
    public long getNumberOfKeys() {
        return delegate.getNumberOfKeys();
    }

    @Override
    public void compact() {
        writeLock.lock();
        try {
            delegate.compact();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public K checkAndRepairConsistency() {
        writeLock.lock();
        try {
            return delegate.checkAndRepairConsistency();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void invalidateIterators() {
        writeLock.lock();
        try {
            delegate.invalidateIterators();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public EntryIterator<K, V> openIterator() {
        readLock.lock();
        try {
            final EntryIterator<K, V> iterator = delegate.openIterator();
            return new LockedEntryIterator<>(iterator, readLock);
        } catch (final Throwable t) {
            readLock.unlock();
            throw t;
        }
    }

    @Override
    public void put(final K key, final V value) {
        writeLock.lock();
        try {
            delegate.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void flush() {
        writeLock.lock();
        try {
            delegate.flush();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public int getNumberOfKeysInWriteCache() {
        return delegate.getNumberOfKeysInWriteCache();
    }

    @Override
    public long getNumberOfKeysInCache() {
        return delegate.getNumberOfKeysInCache();
    }

    @Override
    public V get(final K key) {
        readLock.lock();
        try {
            return delegate.get(key);
        } finally {
            readLock.unlock();
        }
    }

    public <T> T executeWithWriteLock(final Supplier<T> task) {
        writeLock.lock();
        try {
            return task.get();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public SegmentId getId() {
        return delegate.getId();
    }

    @Override
    protected void doClose() {
        try {
            writeLock.lock();
            try {
                delegate.close();
            } finally {
                writeLock.unlock();
            }
        } finally {
            compactionExecutor.shutdownNow();
        }
    }

    private static final class LockedEntryIterator<K, V>
            extends AbstractCloseableResource implements EntryIterator<K, V> {

        private final EntryIterator<K, V> delegate;
        private final Lock lock;

        LockedEntryIterator(final EntryIterator<K, V> delegate,
                final Lock lock) {
            this.delegate = delegate;
            this.lock = lock;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            return delegate.next();
        }

        @Override
        protected void doClose() {
            try {
                delegate.close();
            } finally {
                lock.unlock();
            }
        }
    }

}
