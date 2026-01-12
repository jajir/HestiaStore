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
 * Thread-safe adapter for {@link KeySegmentCache} backed by a read/write lock.
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

    KeySegmentCache.Snapshot<K> snapshot() {
        readLock.lock();
        try {
            return delegate.snapshot();
        } finally {
            readLock.unlock();
        }
    }

    boolean isMappingValid(final K key, final SegmentId expectedSegmentId,
            final long expectedVersion) {
        readLock.lock();
        try {
            return delegate.isMappingValid(key, expectedSegmentId,
                    expectedVersion);
        } finally {
            readLock.unlock();
        }
    }

    boolean isKeyMappedToSegment(final K key,
            final SegmentId expectedSegmentId) {
        readLock.lock();
        try {
            return delegate.isKeyMappedToSegment(key, expectedSegmentId);
        } finally {
            readLock.unlock();
        }
    }

    public SegmentId findNewSegmentId() {
        writeLock.lock();
        try {
            return delegate.findNewSegmentId();
        } finally {
            writeLock.unlock();
        }
    }

    boolean tryExtendMaxKey(final K key,
            final KeySegmentCache.Snapshot<K> snapshot) {
        writeLock.lock();
        try {
            return delegate.tryExtendMaxKey(key, snapshot);
        } finally {
            writeLock.unlock();
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

    void updateSegmentMaxKey(final SegmentId segmentId, final K newMaxKey) {
        writeLock.lock();
        try {
            delegate.updateSegmentMaxKey(segmentId, newMaxKey);
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSegment(final SegmentId segmentId) {
        writeLock.lock();
        try {
            delegate.removeSegment(segmentId);
        } finally {
            writeLock.unlock();
        }
    }

    public Stream<Entry<K, SegmentId>> getSegmentsAsStream() {
        readLock.lock();
        try {
            return delegate.getSegmentsAsStream();
        } finally {
            readLock.unlock();
        }
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
