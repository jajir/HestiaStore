package org.hestiastore.index.sst;

import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

public class EntryIteratorSynchronized<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final EntryIterator<K, V> iterator;
    private final ReentrantLock lock;

    EntryIteratorSynchronized(final EntryIterator<K, V> iterator,
            final ReentrantLock lock) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
        this.lock = Vldtn.requireNonNull(lock, "lock");
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
    public Entry<K, V> next() {
        lock.lock();
        try {
            return iterator.next();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doClose() {
        lock.lock();
        try {
            iterator.close();
        } finally {
            lock.unlock();
        }
    }

}
