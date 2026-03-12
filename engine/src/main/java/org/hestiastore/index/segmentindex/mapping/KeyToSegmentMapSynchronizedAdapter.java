package org.hestiastore.index.segmentindex.mapping;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.split.PartitionSplitApplyPlan;

/**
 * Thread-safe adapter for {@link KeyToSegmentMap} backed by a read/write lock.
 */
public final class KeyToSegmentMapSynchronizedAdapter<K>
        extends AbstractCloseableResource {

    private final KeyToSegmentMap<K> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public KeyToSegmentMapSynchronizedAdapter(
            final KeyToSegmentMap<K> delegate) {
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

    public <T> T withWriteLock(final java.util.function.Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
    }

    public void withWriteLock(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        writeLock.lock();
        try {
            action.run();
        } finally {
            writeLock.unlock();
        }
    }

    public KeyToSegmentMap.Snapshot<K> snapshot() {
        readLock.lock();
        try {
            return delegate.snapshot();
        } finally {
            readLock.unlock();
        }
    }

    public boolean isVersion(final long expectedVersion) {
        readLock.lock();
        try {
            return delegate.isVersion(expectedVersion);
        } finally {
            readLock.unlock();
        }
    }

    public boolean isMappingValid(final K key,
            final SegmentId expectedSegmentId, final long expectedVersion) {
        readLock.lock();
        try {
            return delegate.isMappingValid(key, expectedSegmentId,
                    expectedVersion);
        } finally {
            readLock.unlock();
        }
    }

    public boolean isKeyMappedToSegment(final K key,
            final SegmentId expectedSegmentId) {
        readLock.lock();
        try {
            return delegate.isKeyMappedToSegment(key, expectedSegmentId);
        } finally {
            readLock.unlock();
        }
    }

    public boolean tryExtendMaxKey(final K key,
            final KeyToSegmentMap.Snapshot<K> snapshot) {
        return withWriteLock(() -> delegate.tryExtendMaxKey(key, snapshot));
    }

    /**
     * Inserts a mapping for the provided key, allocating a segment id when
     * needed.
     *
     * @param key key to map
     * @return segment id assigned to the key
     */
    public SegmentId insertKeyToSegment(final K key) {
        return withWriteLock(() -> delegate.insertKeyToSegment(key));
    }

    /**
     * Inserts or updates a mapping for the provided key and segment id.
     *
     * @param key       key to map
     * @param segmentId segment id to associate
     */
    public void insertSegment(final K key, final SegmentId segmentId) {
        withWriteLock(() -> delegate.insertSegment(key, segmentId));
    }

    public void updateSegmentMaxKey(final SegmentId segmentId,
            final K newMaxKey) {
        withWriteLock(() -> delegate.updateSegmentMaxKey(segmentId, newMaxKey));
    }

    public boolean applyPartitionSplitPlan(
            final PartitionSplitApplyPlan<K> plan) {
        return withWriteLock(() -> delegate.applyPartitionSplitPlan(plan));
    }

    /**
     * Removes the mapping for the provided segment id.
     *
     * @param segmentId segment id to remove
     */
    public void removeSegment(final SegmentId segmentId) {
        withWriteLock(() -> delegate.removeSegment(segmentId));
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
        withWriteLock(delegate::optionallyFlush);
    }

    @Override
    protected void doClose() {
        withWriteLock(delegate::close);
    }
}
