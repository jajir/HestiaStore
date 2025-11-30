package org.hestiastore.index.segmentindex;

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorStreamer;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.log.Log;
import org.hestiastore.index.log.LoggedKey;

public class IndexInternalSynchronized<K, V> extends SegmentIndexImpl<K, V> {

    private final ReentrantLock lock = new ReentrantLock();

    public IndexInternalSynchronized(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf, final Log<K, V> log) {
        super(directory, keyTypeDescriptor, valueTypeDescriptor, conf, log);
    }

    @Override
    protected void doClose() {
        lock.lock();
        try {
            super.doClose();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(final K key, final V value) {
        lock.lock();
        try {
            super.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V get(final K key) {
        lock.lock();
        try {
            return super.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(final K key) {
        lock.lock();
        try {
            super.delete(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void compact() {
        lock.lock();
        try {
            super.compact();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Stream<Entry<K, V>> getStream(SegmentWindow segmentWindow) {
        lock.lock();
        try {
            indexState.tryPerformOperation();
            final EntryIterator<K, V> iterator = openSegmentIterator(
                    segmentWindow);
            final EntryIterator<K, V> synchronizedIterator = new EntryIteratorSynchronized<>(
                    iterator, lock);
            final EntryIteratorToSpliterator<K, V> spliterator = new EntryIteratorToSpliterator<K, V>(
                    synchronizedIterator, keyTypeDescriptor);
            return StreamSupport.stream(spliterator, false).onClose(() -> {
                iterator.close();
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public EntryIteratorStreamer<LoggedKey<K>, V> getLogStreamer() {
        lock.lock();
        try {
            return super.getLogStreamer();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush() {
        lock.lock();
        try {
            super.flush();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void checkAndRepairConsistency() {
        lock.lock();
        try {
            super.checkAndRepairConsistency();
        } finally {
            lock.unlock();
        }
    }

}
