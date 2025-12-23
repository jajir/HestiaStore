package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Thread-safe adapter that delegates to a {@link KeySegmentCache} instance.
 */
final class KeySegmentCacheSynchronizedAdapter<K>
        extends AbstractCloseableResource {

    private final KeySegmentCache<K> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    KeySegmentCacheSynchronizedAdapter(final KeySegmentCache<K> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    public void checkUniqueSegmentIds() {
        readLock.lock();
        try {
            delegate.checkUniqueSegmentIds();
        } finally {
            readLock.unlock();
        }
    }

    public SegmentId findSegmentId(final K key) {
        readLock.lock();
        try {
            return delegate.findSegmentId(key);
        } finally {
            readLock.unlock();
        }
    }

    public SegmentId findNewSegmentId() {
        readLock.lock();
        try {
            return delegate.findNewSegmentId();
        } finally {
            readLock.unlock();
        }
    }

    public SegmentId insertKeyToSegment(final K key) {
        writeLock.lock();
        try {
            return delegate.insertKeyToSegment(key);
        } finally {
            writeLock.unlock();
        }
    }

    public void insertSegment(final K key, final SegmentId segmentId) {
        writeLock.lock();
        try {
            delegate.insertSegment(key, segmentId);
        } finally {
            writeLock.unlock();
        }
    }

    public Stream<Entry<K, SegmentId>> getSegmentsAsStream() {
        final List<Entry<K, SegmentId>> snapshot;
        readLock.lock();
        try {
            snapshot = delegate.getSegmentsAsStream().toList();
        } finally {
            readLock.unlock();
        }
        return snapshot.stream();
    }

    public List<SegmentId> getSegmentIds() {
        readLock.lock();
        try {
            return delegate.getSegmentIds();
        } finally {
            readLock.unlock();
        }
    }

    public List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
        readLock.lock();
        try {
            return delegate.getSegmentIds(segmentWindow);
        } finally {
            readLock.unlock();
        }
    }

    public void optionalyFlush() {
        writeLock.lock();
        try {
            delegate.optionalyFlush();
        } finally {
            writeLock.unlock();
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
}
