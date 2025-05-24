package com.hestiastore.index.sst;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;

public class PairIteratorSynchronized<K, V> implements PairIterator<K, V> {

    private final PairIterator<K, V> iterator;
    private final ReentrantLock lock;

    PairIteratorSynchronized(final PairIterator<K, V> iterator,
            final ReentrantLock lock) {
        this.iterator = Objects.requireNonNull(iterator);
        this.lock = Objects.requireNonNull(lock);
    }

    @Override
    public boolean hasNext() {
        lock.lock();
        try {
            return iterator.hasNext();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Pair<K, V> next() {
        lock.lock();
        try {
            return iterator.next();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            iterator.close();
        } finally {
            lock.unlock();
        }
    }

}
