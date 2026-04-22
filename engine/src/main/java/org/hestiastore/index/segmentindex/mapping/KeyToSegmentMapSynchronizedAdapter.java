package org.hestiastore.index.segmentindex.mapping;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.routing.RouteSplitPlan;

/**
 * Thread-safe adapter for {@link KeyToSegmentMapImpl} backed by a read/write
 * lock.
 */
public final class KeyToSegmentMapSynchronizedAdapter<K>
        extends AbstractCloseableResource implements KeyToSegmentMap<K> {

    private final KeyToSegmentMapImpl<K> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public KeyToSegmentMapSynchronizedAdapter(
            final KeyToSegmentMapImpl<K> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    /**
     * Verifies that all segment ids are unique.
     */
    @Override
    public void validateUniqueSegmentIds() {
        readLock.lock();
        try {
            delegate.validateUniqueSegmentIds();
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
    @Override
    public SegmentId findSegmentIdForKey(final K key) {
        readLock.lock();
        try {
            return delegate.findSegmentIdForKey(key);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Snapshot<K> snapshot() {
        readLock.lock();
        try {
            return delegate.snapshot();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isAtVersion(final long expectedVersion) {
        readLock.lock();
        try {
            return delegate.isAtVersion(expectedVersion);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isSnapshotVersionCurrent(final long expectedVersion) {
        readLock.lock();
        try {
            return delegate.isSnapshotVersionCurrent(expectedVersion);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean extendMaxKeyIfNeeded(final K key) {
        writeLock.lock();
        try {
            return delegate.extendMaxKeyIfNeeded(key);
        } finally {
            writeLock.unlock();
        }
    }

    SegmentId insertKeyToSegment(final K key) {
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
     * @param key       key to map
     * @param segmentId segment id to associate
     */
    void insertSegment(final K key, final SegmentId segmentId) {
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

    @Override
    public boolean tryApplySplitPlan(final RouteSplitPlan<K> plan) {
        writeLock.lock();
        try {
            return delegate.tryApplySplitPlan(plan);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Removes the mapping for the provided segment id.
     *
     * @param segmentId segment id to remove
     */
    @Override
    public void removeSegmentRoute(final SegmentId segmentId) {
        writeLock.lock();
        try {
            delegate.removeSegmentRoute(segmentId);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns the segment ids in key order.
     *
     * @return ordered list of segment ids
     */
    @Override
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
    @Override
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
    @Override
    public void flushIfDirty() {
        writeLock.lock();
        try {
            delegate.flushIfDirty();
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
