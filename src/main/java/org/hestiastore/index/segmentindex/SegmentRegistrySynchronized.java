package org.hestiastore.index.segmentindex;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.Segment;

/**
 * SegmentRegistry wrapper that serializes access to the underlying registry.
 * Useful when multiple threads may request segments or close the registry.
 */
class SegmentRegistrySynchronized<K, V> extends SegmentRegistry<K, V> {

    private final ReentrantLock lock = new ReentrantLock();

    SegmentRegistrySynchronized(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf);
    }

    @Override
    public Segment<K, V> getSegment(final SegmentId segmentId) {
        lock.lock();
        try {
            return super.getSegment(segmentId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    boolean isSegmentInstance(final SegmentId segmentId,
            final Segment<K, V> expected) {
        lock.lock();
        try {
            return super.isSegmentInstance(segmentId, expected);
        } finally {
            lock.unlock();
        }
    }

    @Override
    void executeWithRegistryLock(final Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    @Override
    <T> T executeWithRegistryLock(final Supplier<T> action) {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            super.close();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void removeSegment(final SegmentId segmentId) {
        lock.lock();
        try {
            super.removeSegment(segmentId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    void evictSegment(final SegmentId segmentId) {
        lock.lock();
        try {
            super.evictSegment(segmentId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    boolean evictSegmentIfSame(final SegmentId segmentId,
            final Segment<K, V> expected) {
        lock.lock();
        try {
            return super.evictSegmentIfSame(segmentId, expected);
        } finally {
            lock.unlock();
        }
    }
}
