package org.hestiastore.index.cache;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Thread-safe adapter for {@link UniqueCache} backed by a read/write lock.
 */
final class UniqueCacheSynchronizenizedAdapter<K, V> extends UniqueCache<K, V> {

    private final UniqueCache<K, V> delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    UniqueCacheSynchronizenizedAdapter(final UniqueCache<K, V> delegate) {
        super(Vldtn.requireNonNull(delegate, "delegate").getKeyComparator(), 0);
        this.delegate = delegate;
    }

    @Override
    public void put(final Entry<K, V> entry) {
        lock.writeLock().lock();
        try {
            delegate.put(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V get(final K key) {
        lock.readLock().lock();
        try {
            return delegate.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            delegate.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public List<Entry<K, V>> getAsSortedList() {
        lock.readLock().lock();
        try {
            return delegate.getAsSortedList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Entry<K, V>> getAsList() {
        lock.readLock().lock();
        try {
            return delegate.getAsList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Entry<K, V>> snapshotAndClear() {
        lock.writeLock().lock();
        try {
            return delegate.snapshotAndClear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
