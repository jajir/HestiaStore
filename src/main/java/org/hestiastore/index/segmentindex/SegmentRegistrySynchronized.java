package org.hestiastore.index.segmentindex;

import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * SegmentRegistry wrapper that serializes access to the underlying registry.
 * Useful when multiple threads may request segments or close the registry.
 */
class SegmentRegistrySynchronized<K, V> extends SegmentRegistry<K, V> {

    private final ReentrantLock lock = new ReentrantLock();

    SegmentRegistrySynchronized(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        super(directory, keyTypeDescriptor, valueTypeDescriptor, conf);
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
    public void close() {
        lock.lock();
        try {
            super.close();
        } finally {
            lock.unlock();
        }
    }
}
