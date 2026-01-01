package org.hestiastore.index.segment;

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
public class SegmentImplSynchronizationAdapter<K, V>
        extends AbstractCloseableResource implements Segment<K, V> {

    private final Segment<K, V> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(
            true);
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public SegmentImplSynchronizationAdapter(final Segment<K, V> delegate) {
        this.delegate = delegate;
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

}
