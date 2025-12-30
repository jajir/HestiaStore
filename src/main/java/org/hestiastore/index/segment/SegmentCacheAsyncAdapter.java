package org.hestiastore.index.segment;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Thread-safe adapter for {@link SegmentCache} using a read/write lock.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentCacheAsyncAdapter<K, V> {

    private final SegmentCache<K, V> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public SegmentCacheAsyncAdapter(final SegmentCache<K, V> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    public void put(final Entry<K, V> entry) {
        writeLock.lock();
        try {
            delegate.put(entry);
        } finally {
            writeLock.unlock();
        }
    }

    public void put(final K key, final V value) {
        writeLock.lock();
        try {
            delegate.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    public void putToDeltaCache(final Entry<K, V> entry) {
        writeLock.lock();
        try {
            delegate.putToDeltaCache(entry);
        } finally {
            writeLock.unlock();
        }
    }

    public void putToDeltaCache(final K key, final V value) {
        writeLock.lock();
        try {
            delegate.putToDeltaCache(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    public V get(final K key) {
        readLock.lock();
        try {
            return delegate.get(key);
        } finally {
            readLock.unlock();
        }
    }

    public int size() {
        readLock.lock();
        try {
            return delegate.size();
        } finally {
            readLock.unlock();
        }
    }

    public int sizeWithoutTombstones() {
        readLock.lock();
        try {
            return delegate.sizeWithoutTombstones();
        } finally {
            readLock.unlock();
        }
    }

    public void evictAll() {
        writeLock.lock();
        try {
            delegate.evictAll();
        } finally {
            writeLock.unlock();
        }
    }

    public List<Entry<K, V>> getAsSortedList() {
        readLock.lock();
        try {
            return delegate.getAsSortedList();
        } finally {
            readLock.unlock();
        }
    }
}
