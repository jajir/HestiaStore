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
 * Thread-safe adapter for {@link KeyToSegmentMap} backed by a read/write lock.
 */
final class KeyToSegmentMapSynchronizedAdapter<K>
        extends AbstractCloseableResource {

    private final KeyToSegmentMap<K> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    KeyToSegmentMapSynchronizedAdapter(final KeyToSegmentMap<K> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    /**
     * Verifies that all segment ids are unique.
     */
    public void checkUniqueSegmentIds() {
        readLock.lock();
        try {
            delegate.checkUniqueSegmentIds();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Finds the segment id mapped to the provided key.
     *
     * @param key key to look up
     * @return segment id or {@code null} when not mapped
     */
    public SegmentId findSegmentId(final K key) {
        readLock.lock();
        try {
            return delegate.findSegmentId(key);
        } finally {
            readLock.unlock();
        }
    }

    KeyToSegmentMap.Snapshot<K> snapshot() {
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

    /**
     * Allocates a new, unused segment id.
     *
     * @return new segment id
     */
    public SegmentId findNewSegmentId() {
        writeLock.lock();
        try {
            return delegate.findNewSegmentId();
        } finally {
            writeLock.unlock();
        }
    }

    boolean tryExtendMaxKey(final K key,
            final KeyToSegmentMap.Snapshot<K> snapshot) {
        writeLock.lock();
        try {
            return delegate.tryExtendMaxKey(key, snapshot);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Inserts a mapping for the provided key, allocating a segment id when
     * needed.
     *
     * @param key key to map
     * @return segment id assigned to the key
     */
    public SegmentId insertKeyToSegment(final K key) {
        writeLock.lock();
        try {
            return delegate.insertKeyToSegment(key);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Inserts or updates a mapping for the provided key and segment id.
     *
     * @param key key to map
     * @param segmentId segment id to associate
     */
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

    /**
     * Removes the mapping for the provided segment id.
     *
     * @param segmentId segment id to remove
     */
    public void removeSegment(final SegmentId segmentId) {
        writeLock.lock();
        try {
            delegate.removeSegment(segmentId);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns a stream of key-to-segment mappings.
     *
     * @return stream of entries
     */
    public Stream<Entry<K, SegmentId>> getSegmentsAsStream() {
        readLock.lock();
        try {
            return delegate.getSegmentsAsStream();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the segment ids in key order.
     *
     * @return ordered list of segment ids
     */
    public List<SegmentId> getSegmentIds() {
        readLock.lock();
        try {
            return delegate.getSegmentIds();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the segment ids within the provided window.
     *
     * @param segmentWindow window to apply
     * @return ordered list of segment ids
     */
    public List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
        readLock.lock();
        try {
            return delegate.getSegmentIds(segmentWindow);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Flushes the mapping to disk if it has changed.
     */
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
