package com.coroptis.index.sst;

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.log.Log;
import com.coroptis.index.log.LoggedKey;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class IndexInternalSynchronized<K, V> extends SstIndexImpl<K, V> {

    private final ReentrantLock lock = new ReentrantLock();

    public IndexInternalSynchronized(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration conf, final Log<K, V> log) {
        super(directory, keyTypeDescriptor, valueTypeDescriptor, conf, log);
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
    public Stream<Pair<K, V>> getStream(SegmentWindow segmentWindow) {
        lock.lock();
        try {
            indexState.tryPerformOperation();
            final PairIterator<K, V> iterator = openSegmentIterator(
                    segmentWindow);
            final PairIterator<K, V> synchronizedIterator = new PairIteratorSynchronized<>(
                    iterator, lock);
            final PairIteratorToSpliterator<K, V> spliterator = new PairIteratorToSpliterator<K, V>(
                    synchronizedIterator, keyTypeDescriptor);
            return StreamSupport.stream(spliterator, false).onClose(() -> {
                iterator.close();
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public UnsortedDataFileStreamer<LoggedKey<K>, V> getLogStreamer() {
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