package org.hestiastore.index.segment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;

/**
 * Serializes access to a single {@link Segment} instance using a dedicated
 * {@link ReentrantReadWriteLock}. Read operations can run concurrently, while
 * writers/compaction are exclusive and block readers for their duration.
 */
public class SegmentSynchronizationAdapter<K, V> extends AbstractCloseableResource
        implements Segment<K, V> {

    private final Segment<K, V> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final AtomicBoolean compactionScheduled = new AtomicBoolean(false);
    private final AtomicBoolean compactionRerun = new AtomicBoolean(false);

    // Keep compaction off the write path while still honoring the write lock.
    private static final ExecutorService COMPACTION_EXECUTOR = Executors
            .newSingleThreadExecutor(namedThreadFactory("segmentCompaction"));

    public SegmentSynchronizationAdapter(final Segment<K, V> delegate) {
        this.delegate = delegate;
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
        if (segment.wasClosed() || wasClosed()) {
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
        COMPACTION_EXECUTOR.execute(() -> {
            try {
                runCompaction(segment);
                while (compactionRerun.getAndSet(false)) {
                    runCompaction(segment);
                }
            } finally {
                compactionScheduled.set(false);
            }
        });
    }

    private void runCompaction(final SegmentImpl<K, V> segment) {
        if (segment.wasClosed() || wasClosed()) {
            return;
        }
        executeWithWriteLock(() -> {
            if (!segment.wasClosed()) {
                segment.forceCompact();
            }
            return null;
        });
    }

    @Override
    public SegmentStats getStats() {
        readLock.lock();
        try {
            return delegate.getStats();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getNumberOfKeys() {
        readLock.lock();
        try {
            return delegate.getNumberOfKeys();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void optionallyCompact() {
        writeLock.lock();
        try {
            delegate.optionallyCompact();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void forceCompact() {
        writeLock.lock();
        try {
            delegate.forceCompact();
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
        readLock.lock();
        try {
            delegate.invalidateIterators();
        } finally {
            readLock.unlock();
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
    public EntryWriter<K, V> openDeltaCacheWriter() {
        writeLock.lock();
        try {
            final EntryWriter<K, V> writer = delegate.openDeltaCacheWriter();
            return new LockedEntryWriter<>(writer, writeLock);
        } catch (final Throwable t) {
            writeLock.unlock();
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
    public V get(final K key) {
        readLock.lock();
        try {
            return delegate.get(key);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Segment<K, V> createSegmentWithSameConfig(
            final SegmentId segmentId) {
        readLock.lock();
        try {
            return new SegmentSynchronizationAdapter<>(
                    delegate.createSegmentWithSameConfig(segmentId));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SegmentSplitterPolicy<K, V> getSegmentSplitterPolicy() {
        readLock.lock();
        try {
            return delegate.getSegmentSplitterPolicy();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SegmentSplitterResult<K, V> split(final SegmentId segmentId,
            final SegmentSplitterPlan<K, V> plan) {
        writeLock.lock();
        try {
            return delegate.split(segmentId, plan);
        } finally {
            writeLock.unlock();
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
        readLock.lock();
        try {
            return delegate.getId();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getVersion() {
        readLock.lock();
        try {
            return delegate.getVersion();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    protected void doClose() {
        writeLock.lock();
        try {
            delegate.close();
        } finally {
            writeLock.unlock();
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

    private static final class LockedEntryWriter<K, V>
            extends AbstractCloseableResource implements EntryWriter<K, V> {

        private final EntryWriter<K, V> delegate;
        private final Lock lock;

        LockedEntryWriter(final EntryWriter<K, V> delegate,
                final Lock lock) {
            this.delegate = delegate;
            this.lock = lock;
        }

        @Override
        public void write(final Entry<K, V> entry) {
            delegate.write(entry);
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
