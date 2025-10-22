package org.hestiastore.index.sst;

import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;

public class PairIteratorSynchronized<K, V> extends AbstractCloseableResource
        implements PairIterator<K, V> {

    private final PairIterator<K, V> iterator;
    private final ReentrantLock lock;

    PairIteratorSynchronized(final PairIterator<K, V> iterator,
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
    public Pair<K, V> next() {
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
