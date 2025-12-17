package org.hestiastore.index.segment;

import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;

/**
 * Serializes access to a single {@link Segment} instance using a dedicated
 * {@link ReentrantLock}. Iterators and writers are wrapped so that their
 * operations synchronize on the same lock.
 */
public class SegmentSynchronizationAdapter<K, V> extends AbstractCloseableResource
        implements Segment<K, V> {

    private final Segment<K, V> delegate;
    private final ReentrantLock lock = new ReentrantLock();

    public SegmentSynchronizationAdapter(final Segment<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public SegmentStats getStats() {
        lock.lock();
        try {
            return delegate.getStats();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getNumberOfKeys() {
        lock.lock();
        try {
            return delegate.getNumberOfKeys();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void optionallyCompact() {
        lock.lock();
        try {
            delegate.optionallyCompact();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void forceCompact() {
        lock.lock();
        try {
            delegate.forceCompact();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public K checkAndRepairConsistency() {
        lock.lock();
        try {
            return delegate.checkAndRepairConsistency();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public EntryIterator<K, V> openIterator() {
        lock.lock();
        try {
            final EntryIterator<K, V> iterator = delegate.openIterator();
            return new LockedEntryIterator<>(iterator, lock);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public EntryWriter<K, V> openDeltaCacheWriter() {
        lock.lock();
        try {
            final EntryWriter<K, V> writer = delegate.openDeltaCacheWriter();
            return new LockedEntryWriter<>(writer, lock);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V get(final K key) {
        lock.lock();
        try {
            return delegate.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Segment<K, V> createSegmentWithSameConfig(
            final SegmentId segmentId) {
        lock.lock();
        try {
            return new SegmentSynchronizationAdapter<>(
                    delegate.createSegmentWithSameConfig(segmentId));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SegmentSplitterPolicy<K, V> getSegmentSplitterPolicy() {
        lock.lock();
        try {
            return delegate.getSegmentSplitterPolicy();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SegmentSplitter<K, V> getSegmentSplitter() {
        lock.lock();
        try {
            return delegate.getSegmentSplitter();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SegmentId getId() {
        lock.lock();
        try {
            return delegate.getId();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getVersion() {
        lock.lock();
        try {
            return delegate.getVersion();
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

    private static final class LockedEntryIterator<K, V>
            extends AbstractCloseableResource implements EntryIterator<K, V> {

        private final EntryIterator<K, V> delegate;
        private final ReentrantLock lock;

        LockedEntryIterator(final EntryIterator<K, V> delegate,
                final ReentrantLock lock) {
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

    private static final class LockedEntryWriter<K, V>
            extends AbstractCloseableResource implements EntryWriter<K, V> {

        private final EntryWriter<K, V> delegate;
        private final ReentrantLock lock;

        LockedEntryWriter(final EntryWriter<K, V> delegate,
                final ReentrantLock lock) {
            this.delegate = delegate;
            this.lock = lock;
        }

        @Override
        public void write(final Entry<K, V> entry) {
            lock.lock();
            try {
                delegate.write(entry);
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
}
