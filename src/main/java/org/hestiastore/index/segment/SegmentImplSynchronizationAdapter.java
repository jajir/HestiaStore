package org.hestiastore.index.segment;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

/**
 * Serializes access to a single {@link Segment} instance using a dedicated
 * {@link ReentrantReadWriteLock}. Read operations can run concurrently. Writers
 * and compaction take the write lock. Iterators default to acquiring the read
 * lock per {@code hasNext}/{@code next} call, while full-isolation iterators
 * hold the write lock for their entire lifetime.
 */
public class SegmentImplSynchronizationAdapter<K, V>
        extends AbstractCloseableResource
        implements Segment<K, V>, SegmentWriteLockSupport<K, V> {

    private final Segment<K, V> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(
            true);
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final ReentrantLock maintenanceLock = new ReentrantLock(true);

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
    public SegmentResult<Void> compact() {
        maintenanceLock.lock();
        writeLock.lock();
        try {
            if (delegate.wasClosed()) {
                return SegmentResult.closed();
            }
            return delegate.compact();
        } finally {
            writeLock.unlock();
            maintenanceLock.unlock();
        }
    }

    @Override
    public K checkAndRepairConsistency() {
        maintenanceLock.lock();
        writeLock.lock();
        try {
            return delegate.checkAndRepairConsistency();
        } finally {
            writeLock.unlock();
            maintenanceLock.unlock();
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
    public SegmentResult<EntryIterator<K, V>> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    @Override
    public SegmentResult<EntryIterator<K, V>> openIterator(
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            writeLock.lock();
            try {
                final SegmentResult<EntryIterator<K, V>> result = delegate
                        .openIterator(isolation);
                if (!result.isOk()) {
                    return result;
                }
                return SegmentResult.ok(new UnlockingEntryIterator<>(
                        result.getValue(), writeLock));
            } catch (final RuntimeException e) {
                writeLock.unlock();
                throw e;
            }
        } else if (isolation == SegmentIteratorIsolation.FAIL_FAST) {
            readLock.lock();
            SegmentResult<EntryIterator<K, V>> result;
            try {
                result = delegate.openIterator(isolation);
            } finally {
                readLock.unlock();
            }
            if (!result.isOk()) {
                return result;
            }
            return SegmentResult.ok(
                    new LockedEntryIterator<>(result.getValue(), readLock));
        } else {
            throw new IllegalArgumentException(
                    "Unknown isolation level: " + isolation);
        }
    }

    @Override
    public SegmentResult<Void> put(final K key, final V value) {
        if (delegate instanceof SegmentImpl<K, V>) {
            writeLock.lock();
            try {
                return delegate.put(key, value);
            } finally {
                writeLock.unlock();
            }
        }
        writeLock.lock();
        try {
            return delegate.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public SegmentResult<Void> flush() {
        maintenanceLock.lock();
        try {
            if (delegate.wasClosed()) {
                return SegmentResult.closed();
            }
            writeLock.lock();
            try {
                return delegate.flush();
            } finally {
                writeLock.unlock();
            }
        } finally {
            maintenanceLock.unlock();
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
    public SegmentResult<V> get(final K key) {
        readLock.lock();
        try {
            return delegate.get(key);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <T> T executeWithWriteLock(final Supplier<T> task) {
        writeLock.lock();
        try {
            return task.get();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean putIfValid(final Supplier<Boolean> validation,
            final K key, final V value) {
        Vldtn.requireNonNull(validation, "validation");
        writeLock.lock();
        try {
            if (!validation.get()) {
                return false;
            }
            return delegate.put(key, value).isOk();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public <T> T executeWithMaintenanceWriteLock(final Supplier<T> task) {
        maintenanceLock.lock();
        writeLock.lock();
        try {
            return task.get();
        } finally {
            writeLock.unlock();
            maintenanceLock.unlock();
        }
    }

    @Override
    public SegmentId getId() {
        return delegate.getId();
    }

    @Override
    protected void doClose() {
        maintenanceLock.lock();
        writeLock.lock();
        try {
            delegate.close();
        } finally {
            writeLock.unlock();
            maintenanceLock.unlock();
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
            lock.lock();
            try {
                return delegate.hasNext();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Entry<K, V> next() {
            lock.lock();
            try {
                return delegate.next();
            } finally {
                lock.unlock();
            }
        }

        @Override
        protected void doClose() {
            lock.lock();
            try {
                delegate.close();
            } finally {
                lock.unlock();
            }
        }
    }

    private static final class UnlockingEntryIterator<K, V>
            extends AbstractCloseableResource implements EntryIterator<K, V> {

        private final EntryIterator<K, V> delegate;
        private final Lock lock;

        UnlockingEntryIterator(final EntryIterator<K, V> delegate,
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
