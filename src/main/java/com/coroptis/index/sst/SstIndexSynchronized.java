package com.coroptis.index.sst;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import com.coroptis.index.Pair;
import com.coroptis.index.log.LoggedKey;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class SstIndexSynchronized<K, V> implements Index<K, V> {

    private final SstIndexImpl<K, V> index;
    private final ReentrantLock lock = new ReentrantLock();

    SstIndexSynchronized(final SstIndexImpl<K, V> index) {
        this.index = Objects.requireNonNull(index);
    }

    @Override
    public void close() {
        lock.lock();
        try {
            index.close();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(final K key, final V value) {
        lock.lock();
        try {
            index.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V get(final K key) {
        lock.lock();
        try {
            return index.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(final K key) {
        lock.lock();
        try {
            index.delete(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void compact() {
        lock.lock();
        try {
            index.compact();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Stream<Pair<K, V>> getStream() {
        lock.lock();
        try {
            return index.getStreamSynchronized(lock);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public UnsortedDataFileStreamer<LoggedKey<K>, V> getLogStreamer() {
        lock.lock();
        try {
            return index.getLogStreamer();
        } finally {
            lock.unlock();
        }
    }
}
