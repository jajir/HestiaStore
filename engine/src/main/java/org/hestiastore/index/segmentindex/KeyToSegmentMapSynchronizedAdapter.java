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
    void checkUniqueSegmentIds() {
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
    SegmentId findSegmentId(final K key) {
        readLock.lock();
        try {
            return delegate.findSegmentId(key);
        } finally {
            readLock.unlock();
        }
    }

    <T> T withReadLock(final java.util.function.Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        readLock.lock();
        try {
            return action.get();
        } finally {
            readLock.unlock();
        }
    }

    void withReadLock(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        readLock.lock();
        try {
            action.run();
        } finally {
            readLock.unlock();
        }
    }

    <T> T withWriteLock(final java.util.function.Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        writeLock.lock();
        final String previous = System.getProperty(
                "hestiastore.keyMapLockHeld");
        final boolean enforce = Boolean
                .getBoolean("hestiastore.enforceSplitLockOrder");
        if (enforce) {
            System.setProperty("hestiastore.keyMapLockHeld", "true");
        }
        try {
            return action.get();
        } finally {
            if (enforce) {
                if (previous == null) {
                    System.clearProperty("hestiastore.keyMapLockHeld");
                } else {
                    System.setProperty("hestiastore.keyMapLockHeld", previous);
                }
            }
            writeLock.unlock();
        }
    }

    void withWriteLock(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        writeLock.lock();
        final String previous = System.getProperty(
                "hestiastore.keyMapLockHeld");
        final boolean enforce = Boolean
                .getBoolean("hestiastore.enforceSplitLockOrder");
        if (enforce) {
            System.setProperty("hestiastore.keyMapLockHeld", "true");
        }
        try {
            action.run();
        } finally {
            if (enforce) {
                if (previous == null) {
                    System.clearProperty("hestiastore.keyMapLockHeld");
                } else {
                    System.setProperty("hestiastore.keyMapLockHeld", previous);
                }
            }
            writeLock.unlock();
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

    boolean tryExtendMaxKey(final K key,
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
    SegmentId insertKeyToSegment(final K key) {
        return withWriteLock(() -> delegate.insertKeyToSegment(key));
    }

    /**
     * Inserts or updates a mapping for the provided key and segment id.
     *
     * @param key key to map
     * @param segmentId segment id to associate
     */
    void insertSegment(final K key, final SegmentId segmentId) {
        withWriteLock(() -> delegate.insertSegment(key, segmentId));
    }

    void updateSegmentMaxKey(final SegmentId segmentId, final K newMaxKey) {
        withWriteLock(() -> delegate.updateSegmentMaxKey(segmentId, newMaxKey));
    }

    boolean applySplitPlan(final SegmentSplitApplyPlan<K, ?> plan) {
        return withWriteLock(() -> delegate.applySplitPlan(plan));
    }

    /**
     * Removes the mapping for the provided segment id.
     *
     * @param segmentId segment id to remove
     */
    void removeSegment(final SegmentId segmentId) {
        withWriteLock(() -> delegate.removeSegment(segmentId));
    }

    /**
     * Returns a stream of key-to-segment mappings.
     *
     * @return stream of entries
     */
    Stream<Entry<K, SegmentId>> getSegmentsAsStream() {
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
    List<SegmentId> getSegmentIds() {
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
    List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
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
    void optionalyFlush() {
        withWriteLock(delegate::optionalyFlush);
    }

    @Override
    protected void doClose() {
        withWriteLock(delegate::close);
    }
}
